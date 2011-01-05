#!/opt/local/bin/perl -w

###!/usr/bin/perl -w


##
## Weather Snoop XML to mysql DB - nick.*at*.kiwi-hacker.net
##
## v2.00 - Dec 2010 - changes for v2 of weathersnoops xml
## v2.01 - Jan 2011 - local and remote file reading
##

# what does it do?
# read and parse the xml 
# put data into a simple hash table
# simple data manipulation on the hash table
# push hash values into the mysql DB

# ** I've left in loads of debug statements for you to see if it
# ** works, comment them out once you are happy with it.

use Getopt::Long;
use XML::Simple;
use LWP::Simple; 
use Data::Dumper; # used for debugging the XML
use DBI;
use DBD::mysql;
use strict;

# CONFIG VARIABLES
my $platform = "mysql";
my $database = "weather";
my $host = "localhost";
my $port = "3306";
my $user = "weather";
my $pw = "weathersnoop";

# my test DB
$platform = "mysql";
$database = "colinsweather";
$host = "172.30.5.70";
$port = "3306";
$user = "g1gsw";
$pw = "password";


sub usage() {
        print<<USAGE;
WeatherSnoop to mySQL by kiwi-hacker.

Usage:  weathersnoop2mysql.pl --file=<file>

where file is either a local path eg.  /data/weather.xml
or a web address http://server:3001/weather.xml

USAGE
        exit 0;
};

# Tables hash to update with XML values
# We use a hash so we can search it quicker
# These MUST must match the XML names and the mysql table names
# case-sensitive

#
# Note as Boisey's new xml code is totally different, we now
# need to provide a XML path to the value you need for each table
# you can pick English or metric values.  Thus you can also mix them 
# if you wish, saving any conversion at your php or db stage.  ;)
# eg, rain in mm, wind speed in mph, etc.
#

# format is "DB table name" => [ "path","to","value","you","need"],
# capture output from the Data::Dumper below as a debug to find more
my %tables = (
    "barometricPressure"=>["barometricPressure","value","mb"],
    "dayRain"=>["rain","day","value","mm"],
    "indoorDewPoint"=>["dewPoint","indoor","value","C"], 
    "indoorHeatIndex"=>["heatIndex","indoor","value","C"], 
    "indoorHumidity"=>["humidity","indoor","value",'%'],  # note the encapsulated % sign
    "indoorTemperature"=>["temperature","indoor","value","C"],
    "monthRain"=>["rain","month","value","mm"],
    "outdoorDewPoint"=>["dewPoint","outdoor","value","C"],
    "outdoorHeatIndex"=>["heatIndex","outdoor","value","C"],
    "outdoorHumidity"=>["humidity","outdoor","value",'%'], # note the encapsulated % sign
    "outdoorTemperature"=>["temperature","outdoor","value","C"],
    "rainRate"=>["rain","rate","value","mm/hr"],
    "totalRain"=>["rain","total","value","mm"],
    "windChill"=>["windChill","value","C"],
    "windDirection"=>["wind","direction","value","deg"],
    "windGust"=>["wind","gust","value","mph"],
    "windSpeed"=>["wind","speed","value","mph"],
    "yearRain"=>["rain","year","value","mm"],
);
my %data = (); # hash to push the table data into from the XML

my $file="";
my $help=0;
my $xml;

my %options=();

if ( @ARGV == 0 ) {
	usage();
};

Getopt::Long::Configure ('bundling');
GetOptions('f|file=s' => \$file, 'h|help' => \$help);

if ($help==1) {
	usage();
};

if ($file eq "") {
	print "Error no filename provided\n";
	usage();
	exit 1;
};


if ($file=~/^http:/) {

	eval {
		$xml = get $file;
	};
	if (!defined($xml)) {
		print "Error getting xml from the web at $file\n";
		exit 2;
	};

} else {

	if ( -r $file ) {
		$xml=$file;
	} else {
		print "Error opening $file\n";
		exit 3;
	};

};



# we use KeyAttr to frig the xml into a nice hash
my $xs = XML::Simple->new(KeyAttr => { value => 'type' }, forcearray => ['value']);
my $parsed;

eval {
	$parsed = $xs->XMLin($xml);
};
if ($@) {
	print "Error loading xml:  $@\n";
	exit 4;
};

#print Dumper $parsed;  # DEBUG to see XML parsed into perl hash.
#print $parsed->{'dewPoint'}->{'outdoor'}->{'value'}->{'C'}->{'content'};
#exit 0;

foreach my $table ( sort keys %tables ) {

	# create xml path

	# some perl magik ;)
	my @pa=@{$tables{$table}};
	my $path = '$parsed'.join("",map{ sprintf("->{'%s'}", $_)  } @pa).'->{content}';

	#print "$path\n"; # debug

	my $xmlvalue = eval $path;
	$xmlvalue = "error" if $@;

	#print $xmlvalue."\n"; # debug

	#
	# push all read values into this hash so we can
	# fiddle with them and push into the mysql DB
	#	
	$data{$table}=$xmlvalue;
};

# open this up to debug the values
foreach my $xmlvalue ( sort keys %data ) {

	print "$xmlvalue, $data{$xmlvalue}\n";
};

#
# tidy up some anomalies
#

# personally i calculate these on the fly, rather than saving them
# v2 change - i don't like hardcoding the hash keys here, but...
if ($data{"windChill"} == -9999.0) { 
    # use current temperature
    $data{"windChill"} = $data{"outdoorTemperature"};
};
if ($data{"outdoorHeatIndex"} == -9999.0) {
    # use current temperature
    $data{"outdoorHeatIndex"} = $data{"outdoorTemperature"};
};
if ($data{"indoorHeatIndex"} == -9999.0) {
    # use current temperature
    $data{"indoorHeatIndex"} = $data{"indoorTemperature"};
};

#
# open DB
#
my $dsn = "DBI:$platform:$database:$host:$port";
my $dbh = DBI->connect(
    $dsn, $user, $pw,  
    { RaiseError => 1 } 
    ) ||
    die "Database connection not made: $DBI::errstr\n"; 

#
# setup a mysql timestamp 'YYYY-MM-DD HH:MM:SS'
# we do it here, so every value inserted into the DB has the same timestamp
#
my ($sec,$min,$hour,$mday,$mon,$year) = localtime();
my $timestamp = sprintf("%4d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec);

#
# cycle thru all needed values
#
foreach my $table ( sort keys %data ) {

        my $value=$data{$table};

        #print "$table = $value\n";  # debug - to be removed

        #
        # DB insert of data
        #
        # all tables have same schema, luckily

	# but since Boisey's v2 xml change there are duplicate fields  Temp F and Temp C
	# so we have to chop off the _XXXXX values to then push the data into the
	# already built tables. PITA

	$table =~ s/_.*$//; # chop off _xxxxx
        my $query = "INSERT INTO $table (id,time,value) VALUES (DEFAULT, '$timestamp', $value)";
        my $sth = $dbh->prepare($query);

	#print "$query\n"; # debug - to be removed

        my $retval=$sth->execute();

        if (!$retval) {
            die("INSERT Error on DB : $dbh->errstr()\n");
        };

        # that query has ended
        $sth->finish;
};

$dbh->disconnect or warn "Disconnection failed: $!\n";

exit(0);

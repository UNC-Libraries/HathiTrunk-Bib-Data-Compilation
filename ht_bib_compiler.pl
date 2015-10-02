#!/usr/bin/perl
#
# Summary: Takes tab delimited text file (with bnum, item id (barcode), and enumeration/chronology)
#          as input and outputs MARC-XML file of bibliographic records, as per
#          HathiTrust specifications
#
# Usage: perl marc-xml_builder.pl [input file] [output file]
#
# Author: Kristina Spurgin (2015-07-29 - )
#
# Dependencies:
#    /htdocs/connects/afton_iii_iiidba_perl.inc
#
# Important usage notes:
# UTF8 is the biggest factor in this script.  in addition to the use utf8
# declaration at the head of the script, we must also explicitly set the mode of
# any output to utf8.

#***********************************************************************************
# Declarations
#***********************************************************************************

use DBI;
use  DBD::Oracle;
use utf8;
use locale;
use Net::SSH2;
use List::Util qw(first);
use File::Basename;
use Getopt::Long; #allows for use of testing mode, http://perldoc.perl.org/Getopt/Long.html

# set character encoding for stdout to utf8
binmode(STDOUT, ":utf8");

#************************************************************************************
# Set up environment and make sure it is clean
#************************************************************************************
$ENV{'PATH'} = '/bin:/usr/sbin';
delete @ENV{'ENV', 'BASH_ENV'};
$ENV{'NLS_LANG'} = 'AMERICAN_AMERICA.AL32UTF8';

my($dbh, $sth, $sql);

$input = '/htdocs/connects/afton_iii_iiidba_perl.inc';
open (INFILE, "<$input") || die &mail_error("Can't open hidden db connect file\n");

while (<INFILE>) {
    chomp;
    @pair = split("=", $_);
    $mycnf{$pair[0]} = $pair[1];
}

close(INFILE);

my $host = $mycnf{"host"};
my $sid = $mycnf{"sid"};
my $username = $mycnf{"user"};
my $password = $mycnf{"password"};

# untaint all of the db connection variables
if ($host =~ /^([-\@\w.]+)$/) {
    $host=$1;
} else {
    die "Bad data in $host";
}

if ($sid =~ /^([-\@\w.]+)$/) {
    $sid=$1;
} else {
    die "Bad data in $sid";
}

if ($username =~ /^([-\@\w.]+)$/) {
    $username=$1;
} else {
    die "Bad data in $username";
}


$dbh = DBI->connect("dbi:Oracle:host=$host;sid=$sid", $username, $password)
    or die &mail_error("Unable to connect: $DBI::errstr");

# So we don't have to check every DBI call we set RaiseError.
$dbh->{RaiseError} = 1;

#**************************************
# Get your files in order...
#**************************************
#set bnum list
$bnum_file = $ARGV[0];

# open file to write output
# the single most crucial part of this script is to specify the output format as utf8
my $out_path = $ARGV[1];
open(OUTFILE, ">:utf8", "$out_path") or die &mail_error("Couldn't open $out_path for output: $!\n");

my($out_path_file, $out_path_dir, $out_path_ext) = fileparse($out_path);

my $warn_path = "$out_path_dir/bib_errors.txt";
open (WARN, ">:utf8", "$warn_path") or die &mail_error("Couldn't open $warn_path for output: $!\n");

my $warning_ct = 0;

#******************************************
# Build MARC-XML for items in input file
#******************************************
print OUTFILE "<?xml version='1.0'?>\n";
print OUTFILE "<collection xmlns='http://www.loc.gov/MARC21/slim' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd'>\n";

open (INFILE, "<$bnum_file") || die &mail_error("Can't open bnum file: $bnum_file\n");

RECORD: while (<INFILE>) {
    print OUTFILE "  <record>\n";
    chomp;
    my ($bnum_full, $barcode, $volume) = split(/\t/, $_) ;
    $bnum_full = trim($bnum_full);
    $barcode = trim($barcode);
    $volume = trim($volume);
    my $bnum = $bnum_full;
    $bnum =~ s/(.*).$/$1/;

    #Make sure there is a record in the database for the bnum. If not write warning.
    my $bib_ct_sql = "SELECT COUNT (rec_key) FROM BIBLIO2BASE WHERE rec_key = '$bnum'";
    my $bib_ct_sth = $dbh->prepare($bib_ct_sql);
    $bib_ct_sth->execute();
    my $bib_ct;
    $bib_ct_sth->bind_columns (undef, \$bib_ct );
    while ($bib_ct_sth->fetch()) {
        if ($bib_ct == 0) {
            print WARN "$bnum_full\tThere is no bib record with this bib record id. Perhaps the full record id from Millennium was not entered, or there was a copy/paste error?\n";
            $warning_ct += 1;
            print OUTFILE "  </record>\n";
            next RECORD;
        }
    }
    $bib_ct_sth->finish();

    #Make sure record has 1 and only 1 LDR field. If so, write Leader.
    my $ldr_ct_sql = "SELECT COUNT (rec_key) FROM VAR_FIELDS2 WHERE rec_key = '$bnum' AND iii_tag = '_'";
    my $ldr_ct_sth = $dbh->prepare($ldr_ct_sql);
    $ldr_ct_sth->execute();
    my $ldr_ct;
    $ldr_ct_sth->bind_columns (undef, \$ldr_ct );
    while ($ldr_ct_sth->fetch()) {
        if ($ldr_ct == 0) {
            print WARN "$bnum_full\tThis bib record has no Leader. A Leader field is required. Report to cataloging staff to add Leader to record.\n";
            $warning_ct += 1;
        } elsif ($ldr_ct > 1) {
            print WARN "$bnum_full\tThis bib record has more than one Leader field. Only one Leader field is allowed. Report to cataloging staff to add Leader to record.\n";
            $warning_ct += 1;
        } else {
            my $ldr_sql = "SELECT rec_data FROM var_fields2 WHERE rec_key = '$bnum' AND iii_tag = '_'";
            my $ldr_sth = $dbh->prepare($ldr_sql);
            $ldr_sth->execute();
            my $ldr;
            $ldr_sth->bind_columns (undef, \$ldr );
            while ($ldr_sth->fetch()) {
                print OUTFILE "    <leader>$ldr</leader>\n";
                $ldr_length = length ($ldr);
                if ($ldr_length != 24) {
                    print WARN "$bnum_full\tThis Leader in this bib record does not include 24 characters. Report to cataloging staff to fix record.\n";
                    $warning_ct += 1;
                }
            }
            $ldr_sth->finish();
        }
    }
    $ldr_ct_sth->finish();

    #Set up to grab the rest of the fields and process them
    my $bib_sql = "select marc_tag, rec_data, indicator1, indicator2
                    from var_fields2
                    where rec_key = '$bnum' and iii_tag != '_' and marc_tag IS NOT NULL
                    order by marc_tag, rec_seq";

    my $bib_sth = $dbh->prepare($bib_sql);
    $bib_sth->execute();
    my ($marc_tag, $rec_data, $ind1, $ind2) = ('', '', '', '');
    $bib_sth->bind_columns (undef, \$marc_tag, \$rec_data, \$ind1, \$ind2 );

    #Set up counters and things for verification
    my $oclc035 = 0; #set to 1 if 035 contains OCoLC
    my $ct008 = 0; #each 008 field increments count by 1
    my $ct245 = 0; #each 245 field increments count by 1
    my $ct245ak = 0; #incremented by 1 if 245 contains subfield a or k
    my $orig001 = ""; #hold the value from the 001 field in case there's no 035 with OCLC num
    my $orig003 = ""; #hold the record source code to determine if $orig001 is an OCLC num or not

    #provide Hathi-specific 001 and 003
    #Hathi ingests from IA provided bnum without check digit in the 001
    print OUTFILE "      <controlfield tag='001'>$bnum</controlfield>\n";
    print OUTFILE "      <controlfield tag='003'>NcU</controlfield>\n";

  FIELD: while ($bib_sth->fetch()) {
    #Escape XML-reserved characters in the data
        if ($rec_data =~ m/[<>&"']/) {
            $rec_data = escape_xml_reserved ($rec_data);
        }

    #Process control fields
        if ($marc_tag =~ m/00\d/){
            if ($marc_tag =~ m/001/) {
                $orig001 = $rec_data;
            }
            elsif ($marc_tag =~ m/003/) {
                $orig003 = $rec_data;
            }
            else {
                if ($marc_tag == '008') {
                    $ct008 += 1;
                    #III for some ridiculous reason chooses to output the 6 editable LDR bytes on the end of the 008
                    #So those need to be deleted to create valid MARC
                    $rec_data =~ s/^(.*)......$/\1/;
                    my $length008 = length($rec_data);
                    if ($length008 != 40) {
                        print WARN "$bnum_full\tThis bib record's 008 does not have 40 byte positions. Report to cataloging staff to correct 008.\n";
                        $warning_ct += 1;
                    }
                }
                print OUTFILE "      <controlfield tag='$marc_tag'>$rec_data</controlfield>\n";
            }
        }

        #Hathi doesn't need our 9XX fields
        elsif ( $marc_tag =~ m/^9/ ) {
            next FIELD;
        }

        #Process variable fields
        else {
            print OUTFILE "      <datafield ind1='$ind1' ind2='$ind2' tag='$marc_tag'>\n";
            my @subfields = split /\|/, "$rec_data";
            # need to get ordered list of delimiters in fields so we can throw errors
            #  if some fields don't start with (or contain) required subfields
            my @delimiters = ();
            foreach my $subfield (@subfields) {
                if ($subfield) {
                    my $delimiter = substr ($subfield, 0, 1);
                    my $data = trim (substr ($subfield, 1));
                    print OUTFILE "        <subfield code='$delimiter'>$data</subfield>\n";
                    push @delimiters, $delimiter;
                }
            }
            print OUTFILE "      </datafield>\n";

            if ($marc_tag == '245') {
                $ct245 += 1;
                if (first { $_ eq ('a' || 'k') } @delimiters) {
                    $ct245ak += 1;
                }
            }
            if ($marc_tag == '035' && $rec_data =~ m/\|a\(OCoLC\)/) {
                $oclc035 += 1;
            }
        }
    }
    $bib_sth->finish();

    print OUTFILE "      <datafield ind1=' ' ind2=' ' tag='955'>\n";
    print OUTFILE "        <subfield code='b'>$barcode</subfield>\n";
    if ($volume) {
        print OUTFILE "        <subfield code='v'>$volume</subfield>\n";
    }
    print OUTFILE "      </datafield>\n";

    #Check counts of certain fields and write warnings accordingly.
    if ($oclc035 == 0) {
        # If there is no OCLC 035, determine whether 001 is an OCLC number and provide it if possible.
        my $oclcnum;

        # If 001 consists only of digits...
        if ( $orig001 =~ m/^\d+$/ ) {
            #  ...and 003 is blank, then 001 is an OCLC number
            if ( $orig003 =~ m/^$/ ) {
                $oclcnum = $orig001;
            } else { #  ...and 003 is not blank, then if...
                #   ...003 is OCoLC, 001 is OCLC number
                if ( $orig003 =~ m/OCoLC/i ) {
                    $oclcnum = $orig001;
                }
                #   ...003 is not OCoLC, 001 is NOT OCLC number
            }
        } else { # If 001 has characters that are not digits...
            #  ...and 003 is blank, 001 is NOT OCLC number
            #  ...and 003 is not blank, then if...
            unless ( $orig003 =~ m/^$/ ) {
                #   ...003 is OCoLC, remove non-digits and call it OCLC number
                if ( $orig003 =~ m/OCoLC/i ) {
                    $orig001 =~ s/\D//g;
                    $oclcnum = $orig001;
                }
                #   ...003 is not OCoLC, 001 is NOT OCLC number
            }
        }

        if ( $oclcnum ) {
            print OUTFILE "      <datafield ind1=' ' ind2=' ' tag='035'>\n";
            print OUTFILE "        <subfield code='a'>(OCoLC)$oclcnum</subfield>\n";
            print OUTFILE "      </datafield>\n";
        } else {
            print WARN "$bnum_full\tThis bib does not contain an 035 field with an OCLC number. Report to cataloging staff to have an OCLC number added in an 035 field.\n";
            $warning_ct += 1;
        }
    } elsif ($oclc035 > 1) {
        print WARN "$bnum_full\tThis bib contains more than 035 field with an OCLC number. Report to cataloging staff to have an OCLC numbers checked/corrected.\n";
        $warning_ct += 1;
    }

    if ($ct008 == 0) {
        print WARN "$bnum_full\tThis bib does not contain an 008 field, which is a required field. Report to cataloging staff to fix 008 field.\n";
        $warning_ct += 1;
    }

    if ($ct245 > 1) {
        print WARN "$bnum_full\tThis bib contains more than one 245 field, which is a non-repeatable field. Report to cataloging staff to fix.\n";
        $warning_ct += 1;
    }

    if ($ct245ak == 0) {
        print WARN "$bnum_full\tThis bib does not contain a subfield a or k in the 245. Report to cataloging staff to fix.\n";
        $warning_ct += 1;
    }

    print OUTFILE "  </record>\n";
}                               #end RECORD


close(INFILE);

$dbh->disconnect();

print OUTFILE "</collection>";
close(OUTFILE);
close(WARNFILE);

if ( $warning_ct > 0 ) {
    print "Bibliographic metadata compilation failed with $warning_ct errors.\n";
}

sub escape_xml_reserved() {
    my $data = $_[0];
    $data =~ s/&/&amp;/g;
    $data =~ s/</&lt;/g;
    $data =~ s/>/$gt;/g;
    $data =~ s/"/&quot;/g;
    $data =~ s/'/&apos;/g;
    return $data;
}

# Gets rid of white space...
sub trim{
    $incoming = $_[0];
    $incoming =~ s/^\s+//g;
    $incoming =~ s/\s+$//g;
    return $incoming;
}

sub mail_error(){
    $message_addendum = $_[0];
    $message .= $message_addendum;
    $message .= "Compiled bib file not written\n\n";
    print $message;
    exit;
}
exit;

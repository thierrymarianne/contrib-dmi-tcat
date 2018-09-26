<?php

if ($argc < 1)
    die; // only run from command line

include_once __DIR__ . '/../config.php';
include_once __DIR__ . '/../common/constants.php';
include_once __DIR__ . '/../common/functions.php';
include_once __DIR__ . '/../capture/common/functions.php';

// specify the name of the bin here
$bin_name = 'vue_js';

// specify dir with the user timelines (json)
$dir = '/var/www/dmi-tcat/json';
// set type of dump ('import follow' or 'import track')
$type = 'import track';
// if 'import track', specify keywords for which data was captured
$queries = array();

if (empty($bin_name))
    die("bin_name not set\n");

if (dbserver_has_utf8mb4_support() == false) {
    die("DMI-TCAT requires at least MySQL version 5.5.3 - please upgrade your server\n");
}
$querybin_id = queryManagerBinExists($bin_name);

$dbh = pdo_connect();

create_bin($bin_name, $dbh);

queryManagerCreateBinFromExistingTables($bin_name, $querybin_id, $type, $queries);

$all_files = glob("$dir/*.json");

global $tweets_processed, $tweets_failed, $tweets_success,
 $valid_timeline, $empty_timeline, $invalid_timeline, $populated_timeline,
 $total_timeline;

$tweets_processed = $tweets_failed = $tweets_success = $valid_timeline =
$empty_timeline = $invalid_timeline = $populated_timeline = $total_timeline = 0;

$count = count($all_files);
$c = $count;

$stats = (object)[
    'processed' => 0,
    'tweet_ids' => [],
    'all_users' => [],
];

for ($i = 0; $i < $count; ++$i) {
    $filepath = $all_files[$i];
    print "processing $filepath\n";
    process_json_file_timeline($filepath, $dbh);
    print $c-- . "\n";
}

function process_json_file_timeline($filepath, $dbh) {
    global $tweets_processed, $tweets_failed, $tweets_success,
    $valid_timeline, $empty_timeline, $invalid_timeline, $populated_timeline,
    $total_timeline, $bin_name, $stats;

    $tweetQueue = new TweetQueue();

    $total_timeline++;

    ini_set('auto_detect_line_endings', true);

    $contents = file_get_contents($filepath);
    $lines = explode(PHP_EOL, $contents);

    array_walk($lines,
        function ($line) use ($tweetQueue, $bin_name, $stats) {
            $tweet = json_decode(str_replace('\"', "\'", $line), true);

            if ($tweet === null) {
                return;
            }

            $t = new Tweet();
            $t->fromJSON($tweet);
            if (!$t->isInBin($bin_name)) {
                $tweetQueue->push($t, $bin_name);
                if ($tweetQueue->length() > 100) {
                    $tweetQueue->insertDB();
                }

                $stats->all_users[] = $t->from_user_id;
                $stats->tweet_ids[] = $t->id;

                $stats->processed++;
            }
        }
    );

    if ($tweetQueue->length() > 0) {
        $tweetQueue->insertDB();
    }
}

queryManagerSetPeriodsOnCreation($bin_name);

print "\n\n\n\n";
print "Number of tweets: " . count($stats->tweet_ids) . "\n";
print "Unique tweets: " . count(array_unique($stats->tweet_ids)) . "\n";
print "Unique users: " . count(array_unique($stats->all_users)) . "\n";

print "Processed $stats->processed tweets!\n";
//print "Failed storing $tweets_failed tweets!\n";
//print "Succesfully stored $tweets_success tweets!\n";
print "\n";
print "Total number of timelines: $total_timeline\n";
print "Valid timelines: $valid_timeline\n";
print "Invalid timelines: $invalid_timeline\n";
print "Populated timelines: $populated_timeline\n";
print "Empty timelines: $empty_timeline\n";

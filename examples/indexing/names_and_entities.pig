/*
 * DBpedia Spotlight Statistics
 */

-- IMPORTANT: Run this script with "pig -no_multiquery", otherwise the surface forms
-- that are passed to the distributed cache are not available in $TEMPORARY_SF_LOCATION
-- before they are required by pignlproc.helpers.RestrictedNGramGenerator.


SET job.name 'DBpedia Spotlight: Names and entities for $LANG'

%default DEFAULT_PARELLEL 20
SET default_parallel $DEFAULT_PARALLEL

-- enable compression of intermediate results
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

-- SET io.sort.mb 1024
SET mapred.child.java.opts '-Xmx2048m';

-- Make Hadoop a bit more failure-resistant
SET mapred.skip.mode.enabled true;

-- Stop trying after 8 attempts and accept a job if 10% of its mappers fail
SET mapred.map.max.attempts 8;
SET mapred.max.map.failures.percent 10;

SET mapred.reduce.max.attempts 20;
SET mapred.skip.map.max.skip.records 30000;
SET mapred.skip.attempts.to.start.skipping 1;

REGISTER $PIGNLPROC_JAR
DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('$LANG'); -- URI encoding
DEFINE default pignlproc.helpers.SecondIfNotNullElseFirst(); -- default values


--------------------
-- read and count
--------------------
IMPORT '$MACROS_DIR/nerd_commons.pig';

-- Get surfaceForm-URI pairs
ids, articles, pairs = read('$INPUT', '$LANG', $MIN_SURFACE_FORM_LENGTH);

-- Before we count ngrams, write out all the surface forms to a temporary location
-- This has to happen before the ngrams are produced, hence the EXEC statement
allRawSurfaceForms = FOREACH pairs GENERATE
  surfaceForm;
  
-- Add rows for all surfaceforms in lowercase.
allLowerSurfaceForms = FOREACH allRawSurfaceForms GENERATE LOWER(surfaceForm);

-- Join the original surface forms with their lowercased version
allSurfaceForms = UNION allRawSurfaceForms, allLowerSurfaceForms;

STORE allSurfaceForms INTO '$TEMPORARY_SF_LOCATION/surfaceForms';

EXEC;

-- Make ngrams
--pageNgrams = diskIntensiveNgrams(articles, $MAX_NGRAM_LENGTH);
pageNgrams = memoryIntensiveNgrams(articles, pairs, $MAX_NGRAM_LENGTH, '$TEMPORARY_SF_LOCATION', '$LOCALE');

-- Count
uriCounts, sfCounts, pairCounts, ngramCounts = count(pairs, pageNgrams);


--------------------
-- join some results
--------------------

-- Add dummy rows for all surfaceforms in lowercase. Their count is -1 to indicate they are dummy rows
lowercasedSfCounts = FOREACH sfCounts GENERATE LOWER(surfaceForm) AS surfaceForm, -1 as sfCount;

-- Join the original surface forms with their lowercased version
sfCountsWithLowercase = UNION sfCounts, lowercasedSfCounts;

-- Join annotated and unannotated SF counts:
sfAndTotalCounts = FOREACH (JOIN
  sfCountsWithLowercase   BY surfaceForm LEFT OUTER,
  ngramCounts BY ngram) GENERATE surfaceForm, sfCount, ngramCount;


--------------------
-- Output
--------------------

STORE pairCounts INTO '$OUTPUT/pairCounts';
STORE uriCounts INTO '$OUTPUT/uriCounts';
STORE sfAndTotalCounts INTO '$OUTPUT/sfAndTotalCounts';

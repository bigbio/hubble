package com.lordjoe.distributed.hydra.comet_spark;

import com.lordjoe.distributed.*;
import com.lordjoe.distributed.hydra.comet.*;
import com.lordjoe.distributed.hydra.fragment.*;
import com.lordjoe.distributed.hydra.scoring.*;
import com.lordjoe.distributed.hydra.test.*;
import com.lordjoe.utilities.ElapsedTimer;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.systemsbiology.xtandem.*;
import org.systemsbiology.xtandem.ionization.*;
import org.systemsbiology.xtandem.peptide.*;
import org.systemsbiology.xtandem.scoring.*;
import scala.*;

import java.lang.Long;
import java.util.*;

/**
 * com.lordjoe.distributed.hydra.comet_spark.CometScoringHandler
 * do the real work of running the comet algorithm
 * User: Steve
 * Date: 4/10/2015
 */
public class CometScoringHandler extends SparkMapReduceScoringHandler {

    public static final String TOTAL_SCORED_SPECTRA_NAME = "TotalSpectraScored";
    public static final String TOTAL_SCORRED_ACCUMULATOR_NAME = "TotalPeptidesScored";
    public static final String PEPTIDES_ACCUMULATOR_NAME = "PeptideDistribution";
    public static final String SPECTRA_ACCUMULATOR_NAME = "SpectrumDistribution";
    public static final double MINIMUM_ACCEPTABLE_SCORE = 0.01;


    public CometScoringHandler(final String congiguration, final boolean createDb) {

        super(congiguration, createDb);

        XTandemMain application = getApplication();
        CometScoringAlgorithm comet = (CometScoringAlgorithm) application.getAlgorithms()[0];


        comet.configure(application);
    }

    public static class CometCombineScoredScans extends AbstractLoggingFunction2<CometScoredScan, CometScoredScan, CometScoredScan> {
        @Override
        public CometScoredScan doCall(final CometScoredScan s1, final CometScoredScan s2) throws Exception {
            if (s1.getRaw() == null)
                return s2;
            if (s1.getBestMatch() == null)
                return s2;
            if (s2.getRaw() == null)
                return s1;
            if (s2.getBestMatch() == null)
                return s1;
            if (!s1.getId().equals(s2.getId()))
                throw new IllegalStateException("Attempting to combine " + s1.getId() + " and " + s2.getId());
            s1.addTo(s2);
            return s1;
        }
    }

    public static class CombineCometScoringResults extends AbstractLoggingFunction2<CometScoringResult, CometScoringResult, CometScoringResult> {
        @Override
        public CometScoringResult doCall(final CometScoringResult s1, final CometScoringResult s2) throws Exception {
            if (!s1.isValid())
                return s2;
            if (!s2.isValid())
                return s1;
            if (!s1.getId().equals(s2.getId()))
                throw new IllegalStateException("Attempting to combine " + s1.getId() + " and " + s2.getId());
            s1.addTo(s2);
            return s1;
        }
    }

    /**
     * NOIE This class is REALLY important - ALL Comet with peptide lists scoring happens here
     */
    public static class scoreSpectrumAndPeptideList extends AbstractLoggingFlatMapFunction<Tuple2<CometScoredScan, ArrayList<IPolypeptide>>, IScoredScan> {

        private CometScoringAlgorithm comet;
        private Scorer scorer;

        public scoreSpectrumAndPeptideList(XTandemMain application) {
            comet = (CometScoringAlgorithm) application.getAlgorithms()[0];
            scorer = application.getScoreRunner();
        }

        @Override
        public Iterable<IScoredScan> doCall(Tuple2<CometScoredScan, ArrayList<IPolypeptide>> inp) throws Exception {
            List<IScoredScan> ret = new ArrayList<>();
            CometScoredScan scan = inp._1();
            ArrayList<IPolypeptide> peptides = inp._2();

            // This section populates temporary data with the spectrum
            // a lot os free space used temporarily
            CometScoringData.populateFromScan(scan);

            // ===============================


            List<CometTheoreticalBinnedSet> holder = new ArrayList<>();
            for (IPolypeptide peptide : peptides) {
                CometTheoreticalBinnedSet ts = (CometTheoreticalBinnedSet) scorer.generateSpectrum(peptide);
                if (scorer.isTheoreticalSpectrumScored(scan, ts)) {
                    holder.add(ts);
                }
            }
            if (holder.isEmpty())
                return ret; // nothing to score


            CometScoringResult result = new CometScoringResult();
            IMeasuredSpectrum raw = scan.getRaw();
            result.setRaw(raw);
            // use pregenerated peptide data but not epetide data
            double maxScore = 0;
            for (CometTheoreticalBinnedSet ts : holder) {
                IonUseCounter counter = new IonUseCounter();
                double xcorr = comet.doXCorr(ts, scorer, counter, scan, null);
                maxScore = Math.max(xcorr, maxScore);
                if (xcorr > MINIMUM_ACCEPTABLE_SCORE) {
                    IPolypeptide peptide = ts.getPeptide();
                    SpectralMatch spectralMatch = new SpectralMatch(peptide, raw, xcorr, xcorr, xcorr, scan, null);
                    result.addSpectralMatch(spectralMatch);
                }

            }
            if (result.isValidMatch())
                ret.add(result);


            return ret;
        }
    }

    protected static boolean isScanScoredByAnySpectrum(CometTheoreticalBinnedSet ts, Iterable<CometScoredScan> scans, Scorer scorer) {
        for (CometScoredScan scan : scans) {
            if (scorer.isTheoreticalSpectrumScored(scan, ts))
                return true;
        }
        return false; // no one wants to score
    }


    /**
     * NOIE This class is REALLY important - ALL Comet with peptide lists scoring happens here
     * WE CURRENTLY USE THIS CLASS
     */
    public static class ScoreSpectrumAndPeptideWithCogroup extends AbstractLoggingFlatMapFunction<Tuple2<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<HashMap<String, IPolypeptide>>>>, IScoredScan> {

        private CometScoringAlgorithm comet;
        private Scorer scorer;
        private final Accumulator<Long> numberScoredSpectraAccumlator = AccumulatorUtilities.getInstance().createAccumulator(TOTAL_SCORED_SPECTRA_NAME);
        private final Accumulator<Long> numberScoredAccumlator = AccumulatorUtilities.getInstance().createAccumulator(TOTAL_SCORED_SPECTRA_NAME);
        // track bin usage   - make the accumulator exist
        private final Accumulator<MemoryUseAccumulatorAndBinSize> binUsage = AccumulatorUtilities.getInstance().createSpecialAccumulator(MemoryUseAccumulatorAndBinSize.BIN_ACCUMULATOR_NAME,
                MemoryUseAccumulatorAndBinSize.PARAM_INSTANCE, MemoryUseAccumulatorAndBinSize.empty());


        public ScoreSpectrumAndPeptideWithCogroup(XTandemMain application) {
            comet = (CometScoringAlgorithm) application.getAlgorithms()[0];
            scorer = application.getScoreRunner();
        }


        @Override
        public Iterable<IScoredScan> doCall(Tuple2<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<HashMap<String, IPolypeptide>>>> inp) throws Exception {
            List<IScoredScan> ret = new ArrayList<>();
            Iterable<CometScoredScan> scans = inp._2()._1();
            Iterable<HashMap<String, IPolypeptide>> peptidesHashes = inp._2()._2();

            List<CometTheoreticalBinnedSet> holder = new ArrayList<>();
            for (HashMap<String, IPolypeptide> peptideHash : peptidesHashes) {
                ArrayList<IPolypeptide> peptides = new ArrayList<>(peptideHash.values());
                if (peptides.size() > 0) {
                    for (IPolypeptide peptide : peptides) {
                        CometTheoreticalBinnedSet ts = (CometTheoreticalBinnedSet) scorer.generateSpectrum(peptide);
                        if (isScanScoredByAnySpectrum(ts, scans, scorer))
                            holder.add(ts);
                    }
                }
            }
            if (holder.isEmpty())
                return ret; // nothing to score

            long numberScored = 0;

            // This section popul;ates temporary data with the spectrum
            // a lot os free space used temporarily
            for (CometScoredScan scan : scans) {
                CometScoringResult result = new CometScoringResult();
                IMeasuredSpectrum raw = scan.getRaw();
                result.setRaw(raw);

//                int numberGood = 0;
//                 // debugging why do we disagree
//                List<CometTheoreticalBinnedSet> badScore = new ArrayList<CometTheoreticalBinnedSet>();
//                List<CometTheoreticalBinnedSet> notScored = new ArrayList<CometTheoreticalBinnedSet>();
//                List<IPolypeptide> scoredPeptides = new ArrayList<IPolypeptide>();

                CometScoringData.populateFromScan(scan);

                // use pregenerated peptide data but not peptide data
                double maxScore = 0;
                for (CometTheoreticalBinnedSet ts : holder) {

                    if (!scorer.isTheoreticalSpectrumScored(scan, ts))
                        continue;

                    if (TestUtilities.isInterestingPeptide(ts.getPeptide()))
                        TestUtilities.breakHere();

                    IonUseCounter counter = new IonUseCounter();
                    double xcorr = comet.doXCorr(ts, scorer, counter, scan, null);
                    numberScored++;
                    maxScore = Math.max(xcorr, maxScore);


                    IPolypeptide peptide = ts.getPeptide();
                    SpectralMatch spectralMatch = new SpectralMatch(peptide, raw, xcorr, xcorr, xcorr, scan, null);
                    result.addSpectralMatch(spectralMatch);

                }

                //            int testResult = CometTesting.validatePeptideList(scan,scoredPeptides);


                if (result.isValidMatch())
                    ret.add(result);
            }

            if (numberScored > 0) {
                numberScoredAccumlator.add(numberScored);
            }

            return ret;
        }
    }

    /**
     * NOIE This class is REALLY important - ALL Comet with peptide lists scoring happens here
     * WE CURRENTLY USE THIS CLASS
     */
    public static class ScoreSpectrumAndTheoreticalPeptide extends AbstractLoggingFlatMapFunction<Tuple2<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<CometTheoreticalBinnedSet>>>, IScoredScan> {

        private CometScoringAlgorithm comet;
        private Scorer scorer;
        private final Accumulator<Long> numberScoredAccumlator = AccumulatorUtilities.getInstance().createAccumulator(TOTAL_SCORRED_ACCUMULATOR_NAME);


        public ScoreSpectrumAndTheoreticalPeptide(XTandemMain application) {
            comet = (CometScoringAlgorithm) application.getAlgorithms()[0];
            scorer = application.getScoreRunner();
        }


        @Override
        public Iterable<IScoredScan> doCall(Tuple2<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<CometTheoreticalBinnedSet>>> inp) throws Exception {
            List<IScoredScan> ret = new ArrayList<>();
            Iterable<CometScoredScan> scans = inp._2()._1();
            Iterable<CometTheoreticalBinnedSet> holder = inp._2()._2();

            long numberScored = 0;

            // This section popul;ates temporary data with the spectrum
            // a lot os free space used temporarily
            for (CometScoredScan scan : scans) {
                CometScoringResult result = new CometScoringResult();
                IMeasuredSpectrum raw = scan.getRaw();
                result.setRaw(raw);

//                int numberGood = 0;
//                 // debugging why do we disagree
//                List<CometTheoreticalBinnedSet> badScore = new ArrayList<CometTheoreticalBinnedSet>();
//                List<CometTheoreticalBinnedSet> notScored = new ArrayList<CometTheoreticalBinnedSet>();
//                List<IPolypeptide> scoredPeptides = new ArrayList<IPolypeptide>();

                CometScoringData.populateFromScan(scan);

                // use pregenerated peptide data but not peptide data
                double maxScore = 0;
                for (CometTheoreticalBinnedSet ts : holder) {

                    if (!scorer.isTheoreticalSpectrumScored(scan, ts))
                        continue;

//                    if(TestUtilities.isInterestingPeptide(ts.getPeptide()))
//                        TestUtilities.breakHere();

                    IonUseCounter counter = new IonUseCounter();
                    double xcorr = comet.doXCorr(ts, scorer, counter, scan, null);
                    numberScored++;
                    maxScore = Math.max(xcorr, maxScore);


                    IPolypeptide peptide = ts.getPeptide();
                    SpectralMatch spectralMatch = new SpectralMatch(peptide, raw, xcorr, xcorr, xcorr, scan, null);
                    result.addSpectralMatch(spectralMatch);

                }

                //            int testResult = CometTesting.validatePeptideList(scan,scoredPeptides);


                if (result.isValidMatch())
                    ret.add(result);
            }

            if (numberScored > 0) {
                numberScoredAccumlator.add(numberScored);
            }

            return ret;
        }
    }

    /**
     * spectra are scored in multiple bins - this puts them back together
     *
     * @param uncombined
     * @return
     */
    public JavaRDD<CometScoringResult> combineScanScores(JavaRDD<? extends IScoredScan> uncombined) {
        AccumulatorUtilities.getInstance().createAccumulator(TOTAL_SCORED_SPECTRA_NAME);
        AccumulatorUtilities.getInstance().createAccumulator(TOTAL_SCORRED_ACCUMULATOR_NAME);
        // map by scan ids
        JavaPairRDD<String, CometScoringResult> mappedScors = uncombined.mapToPair(new keyScoresByScanId());
        JavaPairRDD<String, CometScoringResult> ret = mappedScors.aggregateByKey(
                new CometScoringResult(),
                new CombineCometScoringResults(),
                new CombineCometScoringResults()
        );
        return ret.values();
    }

    /**
     * NOIE This class is REALLY important - ALL Comet with peptide lists scoring happens here
     */
    public static class ScoreSpectrumAndTheoreticalSpectrumList extends AbstractLoggingFlatMapFunction<Tuple2<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>>, IScoredScan> {

        private CometScoringAlgorithm comet;
        private Scorer scorer;

        public ScoreSpectrumAndTheoreticalSpectrumList(XTandemMain application) {
            comet = (CometScoringAlgorithm) application.getAlgorithms()[0];
            scorer = application.getScoreRunner();
        }

        @Override
        public Iterable<IScoredScan> doCall(Tuple2<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>> inp) throws Exception {
            List<IScoredScan> ret = new ArrayList<>();
            CometScoredScan scan = inp._1();
            ArrayList<CometTheoreticalBinnedSet> holder = inp._2();

            CometScoringData.populateFromScan(scan);


            CometScoringResult result = new CometScoringResult();
            IMeasuredSpectrum raw = scan.getRaw();
            result.setRaw(raw);
            // use pregenerated peptide data but not epetide data
            double maxScore = 0;
            for (CometTheoreticalBinnedSet ts : holder) {
                IonUseCounter counter = new IonUseCounter();
                double xcorr = comet.doXCorr(ts, scorer, counter, scan, null);
                maxScore = Math.max(xcorr, maxScore);
                if (xcorr > MINIMUM_ACCEPTABLE_SCORE) {
                    IPolypeptide peptide = ts.getPeptide();
                    SpectralMatch spectralMatch = new SpectralMatch(peptide, raw, xcorr, xcorr, xcorr, scan, null);
                    result.addSpectralMatch(spectralMatch);
                }

            }
            if (result.isValidMatch())
                ret.add(result);

            return ret;
        }
    }

    private static class keyScoresByScanId<K extends IScoredScan> implements PairFunction<K, String, CometScoringResult> {
        @Override
        public Tuple2<String, CometScoringResult> call(K o) throws Exception {
            CometScoringResult cs = (CometScoringResult) o;
            return new Tuple2<>(o.getId(), cs);
        }
    }


    /**
     * NOIE This class is REALLY important - ALL Comet scoring happens here
     */
    @SuppressWarnings("UnusedDeclaration")
    public class
            CometCombineScoredScanWithScore extends AbstractLoggingFunction2<CometScoringResult, Tuple2<ITheoreticalSpectrumSet, ? extends IScoredScan>, CometScoringResult> {
        @Override
        public CometScoringResult doCall(final CometScoringResult v1, final Tuple2<ITheoreticalSpectrumSet, ? extends IScoredScan> v2) throws Exception {
            //noinspection UnnecessaryLocalVariable
            Tuple2<ITheoreticalSpectrumSet, ? extends IScoredScan> toScore = v2;
            CometScoredScan scoring = (CometScoredScan) toScore._2();
            ITheoreticalSpectrumSet ts = toScore._1();

            XTandemMain application = getApplication();
            Scorer scorer = application.getScoreRunner();
            double xcorr = CometScoringAlgorithm.doRealScoring(scoring, scorer, ts, application);

            IPolypeptide peptide = ts.getPeptide();
            String id = scoring.getId();


            SpectralMatch scan = new SpectralMatch(peptide, scoring, xcorr, xcorr, xcorr, scoring, null);
            //   scoring.addSpectralMatch(scan);

            if (TestUtilities.isCaseLogging()) {
                StringBuilder sb = new StringBuilder();
                double precursorMass = scoring.getPrecursorMass();

                double matchingMass = peptide.getMatchingMass();
                double del = precursorMass - matchingMass;

                //noinspection StringConcatenationInsideStringBufferAppend
                sb.append(scoring.getId() + "\t" + peptide + "\t" + precursorMass + "\t" + matchingMass + "\t" + del + "\t" + xcorr);
                TestUtilities.logCase(sb.toString());
            }

//            if (v1.getRaw() == null)
//                return scoring;
//            v1.addTo(scoring);
//            return v1;

//            CometScoringResult result = new CometScoringResult(scoring.getRaw());
//            result.addSpectralMatch(scan);
            if (!v1.isValid())
                v1.setRaw(scoring.getRaw());
            v1.addSpectralMatch(scan);
            return v1;

        }


    }


    /**
     * NOIE This class is REALLY important - ALL Comet scoring happens here
     */
    public class
            ScoreSpectrumAndPeptide extends AbstractLoggingFlatMapFunction<Tuple2<ITheoreticalSpectrumSet, CometScoredScan>, CometScoringResult> {
        @Override
        public Iterable<CometScoringResult> doCall(Tuple2<ITheoreticalSpectrumSet, CometScoredScan> toScore) throws Exception {
            List<CometScoringResult> ret = new ArrayList<>();
            CometScoredScan scoring = toScore._2();
            ITheoreticalSpectrumSet ts = toScore._1();

            XTandemMain application = getApplication();
            Scorer scorer = application.getScoreRunner();
            double xcorr = CometScoringAlgorithm.doRealScoring(scoring, scorer, ts, application);

            IPolypeptide peptide = ts.getPeptide();

            if (TestUtilities.isInterestingPeptide(peptide))
                TestUtilities.breakHere();

            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            String id = scoring.getId();


            SpectralMatch scan = new SpectralMatch(peptide, scoring, xcorr, xcorr, xcorr, scoring, null);

            scoring.addSpectralMatch(scan);

            if (TestUtilities.isCaseLogging()) {
                StringBuilder sb = new StringBuilder();
                double precursorMass = scoring.getPrecursorMass();

                double matchingMass = peptide.getMatchingMass();
                double del = precursorMass - matchingMass;

                //noinspection StringConcatenationInsideStringBufferAppend
                sb.append(scoring.getId() + "\t" + peptide + "\t" + precursorMass + "\t" + matchingMass + "\t" + del + "\t" + xcorr);
                TestUtilities.logCase(sb.toString());
            }


            CometScoringResult result = new CometScoringResult(scoring.getRaw());
            result.addSpectralMatch(scan);

            ret.add(result);
            return ret;
        }
    }

//    @Override
//    public CometScoredScan doCall(final Tuple2<ITheoreticalSpectrumSet, CometScoredScan> v2) throws Exception {
//        Tuple2<ITheoreticalSpectrumSet, CometScoredScan> toScore = v2;
//        CometScoredScan scoring = toScore._2();
//        ITheoreticalSpectrumSet ts = toScore._1();
//
//        XTandemMain application = getApplication();
//        Scorer scorer = application.getScoreRunner();
//        double xcorr = doRealScoring(scoring, scorer, ts, application);
//
//        IPolypeptide peptide = ts.getPeptide();
//        String id = scoring.getId();
//
//
//        SpectralMatch scan = new SpectralMatch(peptide, scoring, xcorr, xcorr, xcorr, scoring, null);
//        scoring.addSpectralMatch(scan);
//
//        if (TestUtilities.isCaseLogging()) {
//            StringBuilder sb = new StringBuilder();
//            double precursorMass = scoring.getPrecursorMass();
//
//            double matchingMass = peptide.getMatchingMass();
//            double del = precursorMass - matchingMass;
//
//            sb.append(scoring.getId() + "\t" + peptide + "\t" + precursorMass + "\t" + matchingMass + "\t" + del + "\t" + xcorr);
//            TestUtilities.logCase(sb.toString());
//        }
//
//        if (v1.getRaw() == null)
//            return scoring;
//        v1.addTo(scoring);
//        return v1;
//    }
//


    public JavaRDD<? extends IScoredScan> scoreCometBinPairs(final JavaPairRDD<BinChargeKey, Tuple2<ITheoreticalSpectrumSet, CometScoredScan>> binPairs, Partitioner p, long[] countRef) {
        ElapsedTimer timer = new ElapsedTimer();
        XTandemMain application = getApplication();
        CometScoringAlgorithm comet = (CometScoringAlgorithm) application.getAlgorithms()[0];

        // Old code where we first distribute by spectrum then score all peptides
        // this seems to unbalance the load so we will try scoring then mapping
        JavaPairRDD<String, Tuple2<ITheoreticalSpectrumSet, ? extends IScoredScan>> bySpectrumId =
                binPairs.flatMapToPair(new CometMapBinChargeTupleToSpectrumIDTuple(comet));

        if (false)      // use when you want a sample file with the largest spectrum and peptides to score against it
            bySpectrumId = TestUtilities.saveInterestingPairs(bySpectrumId);


        long[] counts = new long[1];
        bySpectrumId = SparkUtilities.persistAndCountPair("By SpectrumID: ", bySpectrumId, counts);
        //  bySpectrumId = SparkUtilities.persistAndCountPair("ScoredPairs", bySpectrumId, countRef);

        int numberPartitions = SparkUtilities.getDefaultNumberPartitions();
        if (p != null)
            numberPartitions = p.numPartitions();

        JavaPairRDD<String, ? extends IScoredScan> scores = bySpectrumId.aggregateByKey(
                new CometScoringResult(),

                new CometCombineScoredScanWithScore(),
                new CombineCometScoringResults()
        );
        return scores.values();
    }

    public JavaRDD<? extends IScoredScan> scoreCometBinPairHash(final JavaPairRDD<BinChargeKey, Tuple2<CometScoredScan, HashMap<String, IPolypeptide>>> binPairs) {
        XTandemMain application = getApplication();
        CometScoringAlgorithm comet = (CometScoringAlgorithm) application.getAlgorithms()[0];

        //  map to a pair dropping bins
        // this is all we need to score
        JavaPairRDD<CometScoredScan, ArrayList<IPolypeptide>> values = binPairs.values().flatMapToPair((PairFlatMapFunction<Tuple2<CometScoredScan, HashMap<String, IPolypeptide>>, CometScoredScan, ArrayList<IPolypeptide>>) tp -> {
            ArrayList<Tuple2<CometScoredScan, ArrayList<IPolypeptide>>> holder = new ArrayList<>();
            CometScoredScan spectrum = tp._1();
            HashMap<String, IPolypeptide> polypeptides = tp._2();
            ArrayList<IPolypeptide> peptides = new ArrayList<>(polypeptides.values());
            if (polypeptides.size() > 0)
                holder.add(new Tuple2<>(spectrum, peptides));

            return holder;
        });

//        values = SparkUtilities.persist(values);
//        List<Tuple2<CometScoredScan, ArrayList<IPolypeptide>>> collect = values.collect();

        JavaRDD<? extends IScoredScan> scores = values.flatMap(new scoreSpectrumAndPeptideList(application));
        return scores;
    }

    public JavaRDD<? extends IScoredScan> scoreCometBinPair(final JavaPairRDD<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<HashMap<String, IPolypeptide>>>> binPairs) {
        XTandemMain application = getApplication();
        JavaRDD<? extends IScoredScan> scores = binPairs.flatMap(new ScoreSpectrumAndPeptideWithCogroup(application));
        return scores;
    }

    public JavaRDD<? extends IScoredScan> scoreCometBinPairPolypeptide(final JavaPairRDD<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<IPolypeptide>>> binPairs) {
        XTandemMain application = getApplication();
        JavaRDD<? extends IScoredScan> scores = binPairs.flatMap(new ScoreSpectrumAndPeptideWithCogroupWithoutHash(application));
        return scores;
    }

    public JavaRDD<? extends IScoredScan> scoreCometBinPairsTheoreticals(final JavaPairRDD<BinChargeKey, Tuple2<Iterable<CometScoredScan>, Iterable<CometTheoreticalBinnedSet>>> binPairs) {
        XTandemMain application = getApplication();
        JavaRDD<? extends IScoredScan> scores = binPairs.flatMap(new ScoreSpectrumAndTheoreticalPeptide(application));
        return scores;
    }

    public JavaRDD<? extends IScoredScan> scoreCometBinPairList(final JavaPairRDD<BinChargeKey, Tuple2<CometScoredScan, ArrayList<IPolypeptide>>> binPairs) {
        XTandemMain application = getApplication();
        CometScoringAlgorithm comet = (CometScoringAlgorithm) application.getAlgorithms()[0];

        //  map to a pair dropping bins
        // this is all we need to score
        JavaPairRDD<CometScoredScan, ArrayList<IPolypeptide>> values = binPairs.values().flatMapToPair((PairFlatMapFunction<Tuple2<CometScoredScan, ArrayList<IPolypeptide>>, CometScoredScan, ArrayList<IPolypeptide>>) tp -> {
            ArrayList<Tuple2<CometScoredScan, ArrayList<IPolypeptide>>> holder = new ArrayList<>();
            CometScoredScan spectrum = tp._1();
            ArrayList<IPolypeptide> polypeptides = tp._2();
            if (polypeptides.size() > 0)
                holder.add(new Tuple2<>(spectrum, polypeptides));

            return holder;
        });

        JavaRDD<? extends IScoredScan> scores = values.flatMap(new scoreSpectrumAndPeptideList(application));
        return scores;
    }

    public JavaRDD<? extends IScoredScan> scoreCometBinTheoreticalPairList(final JavaPairRDD<BinChargeKey, Tuple2<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>>> binPairs) {
        XTandemMain application = getApplication();
        CometScoringAlgorithm comet = (CometScoringAlgorithm) application.getAlgorithms()[0];

        //  map to a pair dropping bins
        // this is all we need to score
        JavaPairRDD<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>> values = binPairs.values().flatMapToPair((PairFlatMapFunction<Tuple2<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>>, CometScoredScan, ArrayList<CometTheoreticalBinnedSet>>) tp -> {
            ArrayList<Tuple2<CometScoredScan, ArrayList<CometTheoreticalBinnedSet>>> holder = new ArrayList<>();
            CometScoredScan spectrum = tp._1();
            ArrayList<CometTheoreticalBinnedSet> polypeptides = tp._2();
            if (polypeptides.size() > 0)
                holder.add(new Tuple2<>(spectrum, polypeptides));

            return holder;
        });

        JavaRDD<? extends IScoredScan> scores = values.flatMap(new ScoreSpectrumAndTheoreticalSpectrumList(application));
        return scores;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static class CometMapBinChargeTupleToSpectrumIDTuple<T extends IMeasuredSpectrum> extends AbstractLoggingPairFlatMapFunction<Tuple2<BinChargeKey, Tuple2<ITheoreticalSpectrumSet, T>>, String, Tuple2<ITheoreticalSpectrumSet, T>> {
        private final CometScoringAlgorithm comet;

        public CometMapBinChargeTupleToSpectrumIDTuple(final CometScoringAlgorithm pComet) {
            comet = pComet;
        }


        @Override
        public Iterable<Tuple2<String, Tuple2<ITheoreticalSpectrumSet, T>>> doCall(final Tuple2<BinChargeKey, Tuple2<ITheoreticalSpectrumSet, T>> t) throws Exception {
            List<Tuple2<String, Tuple2<ITheoreticalSpectrumSet, T>>> holder = new ArrayList<>();

            Tuple2<ITheoreticalSpectrumSet, T> pair = t._2();
            IMeasuredSpectrum spec = pair._2();
            IPolypeptide pp = pair._1().getPeptide();

            // if we dont score give up
            boolean pairScored = comet.isPairScored(spec, pp);
            if (!pairScored) {

                if (TestUtilities.isInterestingPeptide(pp))
                    //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
                    pairScored = comet.isPairScored(spec, pp); // repeat and look
                if (TestUtilities.isInterestingSpectrum(spec))
                    //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
                    pairScored = comet.isPairScored(spec, pp);   // repeat and look


                return holder;
            }

            String id = spec.getId();
            holder.add(new Tuple2<>(id, pair));
            return holder;

        }


    }


}

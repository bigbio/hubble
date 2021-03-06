package com.lordjoe.distributed.hydra.comet;

import com.lordjoe.distributed.hydra.test.TestUtilities;
import org.junit.Assert;
import org.junit.Test;
import org.systemsbiology.xtandem.RawPeptideScan;
import org.systemsbiology.xtandem.XTandemMain;
import org.systemsbiology.xtandem.peptide.IPolypeptide;
import org.systemsbiology.xtandem.scoring.Scorer;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * com.lordjoe.distributed.hydra.comet.CometBinningTest
 *
 * @author Steve Lewis
 * @date 5/22/2015
 */
public class CometBinningTest {

    public static final double REQUIRED_PRECISION = 0.001;


    @Test
    public void testIonBinning() throws Exception {
        XTandemMain application = CometTestingUtilities.getDefaultApplication();
        CometScoringAlgorithm comet = CometTestingUtilities.getComet(application);
        Scorer scorer = application.getScoreRunner();
        MassDifferenceAccumulator mds = new MassDifferenceAccumulator();

        // this version will make test bins
         Map<IPolypeptide, List<BinnedChargeIonIndex>> cBons = CometTestingUtilities.readCometBinsFromResource("/CometAssignedBins.txt");
        for (IPolypeptide pp : cBons.keySet()) {
            List<BinnedChargeIonIndex> bins = cBons.get(pp);
            validateBins(pp, bins,comet,scorer,mds);
        }
    }

    @Test
    public void testEG3_20IonBinning() throws Exception {
        XTandemMain application = CometTestingUtilities.getDefaultApplication();
        CometScoringAlgorithm comet = CometTestingUtilities.getComet(application);
        Scorer scorer = application.getScoreRunner();
        MassDifferenceAccumulator mds = new MassDifferenceAccumulator();
        int totalBins = 0;
        int totalBadBins = 0;
        // this version will make test bins
         Map<IPolypeptide, List<BinnedChargeIonIndex>> cBons = CometTestingUtilities.readCometBinsFromResource("/eg3_20/Scoring_EG20.txt");
        for (IPolypeptide pp : cBons.keySet()) {
            List<BinnedChargeIonIndex> bins = cBons.get(pp);
            totalBins += bins.size();
            totalBadBins += validateBins(pp, bins,comet,scorer,mds);
        }
              Assert.assertTrue(totalBadBins < totalBins / 100);

    }


    private static int numberTested;
    private static int numberRun;

    private int validateBins(IPolypeptide pp, List<BinnedChargeIonIndex> bins,CometScoringAlgorithm comet, Scorer scorer, MassDifferenceAccumulator mds ) {
        int numberFailed = 0;
        int testCharge = 1;
        for (BinnedChargeIonIndex bin : bins) {
            testCharge = Math.max(bin.charge,testCharge);
        }
        Collections.sort(bins,BinnedChargeIonIndex.BY_INDEX);
        numberRun++;

        double matchingMass = pp.getMatchingMass();
        CometTheoreticalBinnedSet ts = new CometTheoreticalBinnedSet(testCharge,matchingMass, pp, comet, scorer);
       List<BinnedChargeIonIndex> hydraFinds = ts.getBinnedIndex(comet, null);

        Collections.sort(hydraFinds,BinnedChargeIonIndex.BY_INDEX);

        if(bins.size() != hydraFinds.size())
             Assert.assertEquals(bins.size(), hydraFinds.size());
        int index = 0;
        for (BinnedChargeIonIndex bin : bins) {
            numberTested++;
            BinnedChargeIonIndex bin2 = hydraFinds.get(index++);
    //        TestBinChargeIonIndex bin1 = (TestBinChargeIonIndex) bin;
   //         TestBinChargeIonIndex bin21 = (TestBinChargeIonIndex) bin2;
    //        int mdBin = mds.accumulateDifference(bin1.mass, bin2.mass,bin2.type);
            if(bin.index != bin2.index) {
                ts = new CometTheoreticalBinnedSet(testCharge,matchingMass, pp, comet, scorer);
                if(Math.abs(bin.index - bin2.index) > 1)
                    Assert.assertEquals(bin.index, bin2.index);
                numberFailed++;
                if(numberFailed > 50)
                     Assert.assertEquals(bin.index, bin2.index);
            }
        }
       if(numberFailed > 0)
           TestUtilities.breakHere();
        return numberFailed;
    }


    @Test
    public void testMasses() throws Exception {
        List<UsedSpectrum> spectrumUsed = CometTestingUtilities.getSpectrumUsed(8852);
        Assert.assertEquals(311, spectrumUsed.size());
        for (UsedSpectrum usedSpectrum : spectrumUsed) {
            validatePeptideMass(usedSpectrum);
        }

        UsedSpectrum one = spectrumUsed.get(0);
        RawPeptideScan spec = CometTestingUtilities.getScanFromMZXMLResource("/000000008852.mzXML");
        Assert.assertEquals(one.spectrumMass, spec.getPrecursorMass(), REQUIRED_PRECISION);
    }

    @Test
    public void testBins() throws Exception {
        CometTesting.validateOneKey(); // We are hunting for when this stops working

        List<UsedSpectrum> spectrumUsed = CometTestingUtilities.getSpectrumUsed(8852);
        RawPeptideScan spec = CometTestingUtilities.getScanFromMZXMLResource("/000000008852.mzXML");
        CometTestingUtilities.doBinTest(spectrumUsed, spec);
    }

    @Test
    public void testWithInit() throws Exception {
        List<UsedSpectrum> spectrumUsed = CometTestingUtilities.getSpectrumUsed(8852);
        RawPeptideScan spec = CometTestingUtilities.getScanFromMZXMLResource("/000000008852.mzXML");
        CometTestingUtilities.doBinTest(spectrumUsed, spec);


        XTandemMain.setShowParameters(false);  // I do not want to see parameters
        XTandemMain application = CometTestingUtilities.getDefaultApplication();
        CometScoringAlgorithm comet = CometTestingUtilities.getComet(application);
          CometTestingUtilities.doBinTest(spectrumUsed, spec);


        CometTestingUtilities.doBinTest(spectrumUsed, spec);

           CometScoredScan scan = new CometScoredScan(spec, comet);

        CometTestingUtilities.doBinTest(spectrumUsed, spec);
        CometTestingUtilities.doBinTest(spectrumUsed, scan);


        RawPeptideScan spec2 = CometTestingUtilities.getScanFromMZXMLResource("/000000008852.mzXML");
        CometTestingUtilities.doBinTest(spectrumUsed, spec2);
        CometScoredScan scan2 = new CometScoredScan(spec, comet);

        CometTestingUtilities.doBinTest(spectrumUsed, scan2);

    }

    private void validatePeptideMass(UsedSpectrum spc) {
        double expected = spc.peptideMass;
        double seen = getMass(spc.peptide);
        Assert.assertEquals(expected, seen, REQUIRED_PRECISION);

    }

    private double getMass(IPolypeptide pp) {
        return CometScoringAlgorithm.getCometMatchingMass(pp);

    }
}

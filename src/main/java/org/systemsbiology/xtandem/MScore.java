package org.systemsbiology.xtandem;

import org.systemsbiology.hadoop.*;

import java.io.*;
import java.util.*;

/**
 * org.systemsbiology.xtandem.MScore
 *
 * @author Steve Lewis
 * @date Dec 21, 2010
 */
/*
 * mscore is the class that contains most of the logic for comparing one sequence with
 * many tandem mass spectra. mprocess contains a comprehensive example of how to use an mscore
 * class. mscore has been optimized for speed, without any specific modifications to take
 * advantage of processor architectures.
 */
public class MScore
{
    public static MScore[] EMPTY_ARRAY = {};
    public static Class THIS_CLASS = MScore.class;

    private float m_fErr; // error for the fragment ions
    private float m_fHomoError;
    private float m_fHyper; // current hyper score
    private float m_fParentErrMinus; // error for the m_ParentStream ion M+H (not m/z)
    private float m_fParentErrPlus; // error for the m_ParentStream ion M+H (not m/z)
    private float m_fMaxMass;
    private float m_fMinMass;
    private int m_lMaxCharge; // current m_ParentStream ion charge
    private int m_lMaxPeaks; // if > 0, the m_lMaxPeaks most intense peaks will be used
    private double m_dScale; // scale for use in hconvert
    private SequenceUtilities m_seqUtil; // class contains variables and constants for calculating sequence and
    // fragment masses
    private SequenceUtilities m_seqUtilAvg; // class contains variables and constants for calculating fragment masses
    // based on average atomic masses
    private SequenceUtilities m_pSeqUtilFrag; // pointer to the msequtilities object to use for fragment ion masses
    private CoreState m_State; // class stores information about the potential modification state machine
    private ScorePAM m_Pam; // class stores information about point mutations state machine
    private ScoreAP m_Sap; // class stores information about single amino acid polymorphisms state machine
    private MScoreTerm m_Term; // class stores information about potential modification of the N- & C-terminii
    private int[] m_plCount = new int[16];// ion count information, indexed using the mscore_type_a enum
    private float[] m_pfScore = new float[16];// convolute score information, indexed using the mscore_type_a enum

    private int m_lType; // current ion type - value from mscore_type

    private boolean m_bUsePam; // true if the peptide will be checked for all possible point mutations
    private boolean m_bUseSaps; // true if the peptide will be checked for all known single amino acid polymorphisms
    private boolean m_bIsC; // true if the current peptide contains the C-terminus of the protein
    private boolean m_bIsN; // true if the current peptide contains the N-terminus of the protein
    private boolean m_bIsotopeError; // true if the spectrum mass may be associated with the wrong isotopic peak
    private int m_lMILength; // current length of the mi vector
    private int m_lSeqLength; // current sequence length
    private int m_lSize; // maximum sequence length - this can be adjusted on the fly
    private int m_lSpectra; // current length of the m_vSpec vector
    private int m_lErrorType; // current ion mass accuracy information - value from mscore_error
    private float m_fScore; // current convolution score
    private double m_dSeqMH; // current sequence M+H - changed from m_fSeqMH to improve accuracy of m_ParentStream ion mass calculations
    private float m_fWidth; // current half-width of the entry for a single fragment ion in the m_vsmapMI map
    // this value is used by blur
    private float[] m_pfSeq; // residue masses corresponding to the current sequence in daltons
    private int[] m_plSeq; // residue masses corresponding to the current sequence, converted into integers
    private char[] m_pSeq; // the current sequence
    private int m_lId; // id of the current spectrum
    private int m_tSeqPos; // zero-based absolute position of the current peptide in the protein sequence
    private int m_lDetails;
    private int m_iCharge;
    private boolean m_bMini;

    private final List<Spectrum> m_vSpec = new ArrayList<Spectrum>(); // vector of all spectra being considered
    // for all spectra being considered
    private final List<SpectrumDetails> m_vDetails = new ArrayList<SpectrumDetails>(); // vector of mspectrumdetails objects, for looking up m_ParentStream ion M+H
    // values of mass spectra
    private final Set<SpectrumIndex> m_sIndex = new HashSet<SpectrumIndex>();
    ;
    private PermuteState m_psPermute;


/*
 * global less than operator for mspectrumdetails classes: to be used in sort operations
 */

    public static class LessThanDetails implements Comparator<SpectrumDetails>,Serializable {
         @Override
        public int compare(SpectrumDetails o1, SpectrumDetails o2)
        {
            if(o1 == o2) return 0;
            final float f1 = o1.getfL();
            final float f2 = o2.getfL();
            if(f1 != f2)
                return f1 < f2 ? -1 : 1;

            final float i1 = o1.getlA();
              final float i2 = o2.getlA();
              if(i1 != i2)
                  return i1 < i2 ? -1 : 1;
             return 0;
        }
    }





    public MScore(IParameterHolder params)
    {
        m_seqUtilAvg = new SequenceUtilities(MassType.average,params);

        m_seqUtil = new SequenceUtilities(MassType.monoisotopic,params);

        m_pSeqUtilFrag = m_seqUtil;    // default to monoisotopic masses for fragment ions


        m_lType = ScoreType.T_Y | ScoreType.T_B;
        m_lErrorType = ScoreError.T_PARENT_DALTONS | ScoreError.T_FRAGMENT_DALTONS;
        m_fParentErrPlus = 2.0F;
        m_fParentErrMinus = 2.0F;
        m_fErr = (float) 0.45;

        m_fWidth = 1.0F;
        m_lMaxCharge = 100;
        m_dSeqMH = -1.0;
        m_lSize = 256;
        m_lSeqLength = 0;  // bpratt 9-20-2010
        m_pfSeq = new float[m_lSize];
        m_plSeq = new int[m_lSize];
        m_pSeq = new char[m_lSize];
        m_bIsotopeError = false;
  
        m_fMinMass = 0.0F;
        m_fMaxMass = 1.0F;
        m_bUsePam = false;
        m_bUseSaps = false;  // bpratt 9/30/2010
        m_fHomoError = 4.5F;
        m_dScale = 1.0;
        m_bMini = false;
        m_iCharge = 1;
    }


    boolean set_mini(final boolean _b)
    {
        m_bMini = _b;
        return m_bMini;
    }

/*
 * allows score object to issue warnings, or set variable based on xml.
 * default implementation does nothing.
 */

    boolean load_param(Map<String, String> _x)
    {
        String strKey = "spectrum, fragment mass type";
        String strValue = _x.get(strKey);
        if ("average".equals(strValue)) {
            set_fragment_masstype(MassType.average);
        }

        return true;
    }

/*
 * called before spectrum conditioning to allow the score object to
 * modify the spectrum in ways specific to the scoring algorithm.
 * default implementation does nothing.
 */

    boolean precondition(Spectrum _s)
    {
        return true;
    }

/*
 * called before scoring inside the score() function to allow any
 * necessary resetting of member variables.
 */

    void prescore(final int _i)
    {
        m_lId = _i;
        m_fHyper = (float) 0.0;
    }

    boolean clear()
    {
        m_vSpec.clear(); // vector of all spectra being considered
        // for all spectra being considered
        m_vDetails.clear(); // vector of mspectrumdetails objects, for looking up m_ParentStream ion M+H
        // values of mass spectra
        return true;
    }
/*
 * create list of non-zero predicted intensity values for a-ions and their
 * integer converted m/z values
 */

    boolean add_A(final long _t, final long _c)
    {

        int a = 0;
/*
 * get the conversion factor between a straight sequence mass and an a-ion
 */
        double dValue = m_pSeqUtilFrag.getdA(); // - > m_dA;
/*
 * deal with protein N-terminus
 */
        if (m_bIsN) {
            dValue += m_pSeqUtilFrag.getfNT(); // - > m_fNT;
        }
/*
 * deal with non-hydrolytic cleavage
 */
        dValue += (m_pSeqUtilFrag.getdCleaveNdefault() - m_pSeqUtilFrag.getdCleaveCdefault()); // - > m_dCleaveNdefault);
        if (m_Term.getlN() != 0) {
            dValue += m_pSeqUtilFrag.getPdAaMass()[(int) '[']; // - > m_pdAaMod['['];
        }
        dValue += m_pSeqUtilFrag.getPdAaFullMod()[(int) '[']; //- > m_pdAaFullMod['['];

        int lValue = 0;
/*
 * calculate the conversion factor between an m/z value and its integer value
 * as referenced in m_vsmapMI
 */
        char cValue = '\0';
        float[] pfScore = m_pSeqUtilFrag.getPfAScore(); // - > m_pfAScore;

        int lCount = 0;
/*
 * from N- to C-terminus, calcuate fragment ion m/z values and store the results
 * look up appropriate scores from m_pSeqUtilFrag->m_pfAScore
 */
        int tPos = m_tSeqPos;
        while (a < m_lSeqLength) {
            cValue = m_pSeq[a];
            dValue += m_pSeqUtilFrag.getAaMass(cValue,
                    tPos + a); // - > getAaMass(cValue, tPos + a);
            lValue = mconvert(dValue, _c);
            m_plSeq[lCount] = lValue;
            m_pfSeq[lCount] = pfScore[cValue];
            lCount++;
            a++;
        }
/*
 * set the next integer mass value to 0: this marks the end of the array 
 */
        m_plSeq[lCount] = 0;
        return true;
    }

    private double incrementDValue(double dValue)
    {
        final int index = (int) '[';
        if (m_Term.getlN() != 0) {
            if (m_Term.getlN() != 0) {
                dValue += m_pSeqUtilFrag.getPdAaMass()[index]; // - > m_pdAaMod['['];
            }
            dValue += m_pSeqUtilFrag.getPdAaFullMod()[index]; //- > m_pdAaFullMod['['];
        }
        return dValue;
     }
/*
 * create list of non-zero predicted intensity values for b-ions and their
 * integer converted m/z values
 */

    boolean add_B(final long _t, final long _c)
    {

        int a = 0;
/*
 * get the conversion factor between a straight sequence mass and a b-ion
 */
        double dValue = m_pSeqUtilFrag.getdB(); // - > m_dB;
/*
 * deal with protein N-terminus
 */
        if (m_bIsN) {
            dValue += m_pSeqUtilFrag.getfNT(); //  - > m_fNT;
        }
/*
 * deal with non-hydrolytic cleavage
 */
        dValue += (m_pSeqUtilFrag.getdCleaveNdefault() - m_pSeqUtilFrag.getdCleaveNdefault()); //  - > m_dCleaveNdefault);
        dValue = incrementDValue(dValue);

        int lValue = 0;
/*
 * calculate the conversion factor between an m/z value and its integer value
 * as referenced in m_vsmapMI
 */
        char cValue = '\0';
        int lCount = 0;
        float[] pfScore = m_pSeqUtilFrag.getPfBScore(); // - > m_pfBScore;
        float[] pfScorePlus = m_pSeqUtilFrag.getPfYScore(); // - > m_pfYScore;
/*
 * from N- to C-terminus, calcuate fragment ion m/z values and store the results
 * look up appropriate scores from m_pSeqUtilFrag->m_pfBScore
 */
        final
        int tPos = m_tSeqPos;
        while (a < m_lSeqLength - 1) {
            cValue = m_pSeq[a];
            dValue += m_pSeqUtilFrag.getAaMass(cValue,
                    tPos + a); // - > getAaMass(cValue, tPos + a);
            lValue = mconvert(dValue, _c);
            m_plSeq[lCount] = lValue;
            m_pfSeq[lCount] = pfScore[cValue] * pfScorePlus[m_pSeq[a + 1]];
            if (a == 1) {
                if (m_pSeq[1] == 'P') {
                    m_pfSeq[lCount] *= 10;
                }
                else {
                    m_pfSeq[lCount] *= 3;
                }
            }
            lCount++;
            a++;
        }
        m_plSeq[lCount] = 0;
        return true;
    }
/*
 * create list of non-zero predicted intensity values for c-ions and their
 * integer converted m/z values
 */

    boolean add_C(final long _t, final long _c)
    {

        int a = 0;
/*
 * get the conversion factor between a straight sequence mass and a b-ion
 */
        double dValue = m_pSeqUtilFrag.getdC(); // - > m_dC;
/*
 * deal with protein N-terminus
 */
        if (m_bIsN) {
            dValue += m_pSeqUtilFrag.getfNT(); //  - > m_fNT;
        }
/*
 * deal with non-hydrolytic cleavage
 */
        dValue += (m_pSeqUtilFrag.getdCleaveNdefault() - m_pSeqUtilFrag.getdCleaveNdefault());
        dValue = incrementDValue(dValue);

        int lValue = 0;
/*
 * calculate the conversion factor between an m/z value and its integer value
 * as referenced in m_vsmapMI
 */
        char cValue = '\0';
        int lCount = 0;
        float[] pfScore = m_pSeqUtilFrag.getPfBScore(); //  - > m_pfBScore;
        float[] pfScorePlus = m_pSeqUtilFrag.getPfYScore(); //  - > m_pfYScore;
/*
 * from N- to C-terminus, calcuate fragment ion m/z values and store the results
 * look up appropriate scores from m_pSeqUtilFrag->m_pfBScore
 */
        final
        int tPos = m_tSeqPos;
        while (a < m_lSeqLength - 2) {
            cValue = m_pSeq[a];
            dValue += m_pSeqUtilFrag.getAaMass(cValue, tPos + a);
            lValue = mconvert(dValue, _c);
            m_plSeq[lCount] = lValue;
            m_pfSeq[lCount] = pfScore[cValue] * pfScorePlus[m_pSeq[a + 1]];
            lCount++;
            a++;
        }
        m_plSeq[lCount] = 0;
        return true;
    }
/*
 * create list of non-zero predicted intensity values for x-ions and their
 * integer converted m/z values
 */

    boolean add_X(final long _t, final long _c)
    {
        int a = m_lSeqLength - 1;
/*
 * get the conversion factor between a straight sequence mass and an x-ion
 */
        double dValue = m_pSeqUtilFrag.getdX(); //  - > m_dX;
/*
 * deal with non-hydrolytic cleavage
 */
        dValue += (m_pSeqUtilFrag.getdCleaveNdefault() - m_pSeqUtilFrag.getdCleaveCdefault()); // - > m_dCleaveCdefault);
        dValue = incrementDValue(dValue);
 /*
 * deal with protein C-teminus
 */
        if (m_bIsC) {
            dValue += m_pSeqUtilFrag.getfCT(); //  - > m_fCT;
        }

        int lValue = 0;
/*
 * calculate the conversion factor between an m/z value and its integer value
 * as referenced in m_vsmapMI
 */
        char cValue = '\0';

        int lCount = 0;
        float fSub = 0.0F;
        float[] pfScore = m_pSeqUtilFrag.getPfXScore(); // - > m_pfXScore;
/*
 * from C- to N-terminus, calcuate fragment ion m/z values and store the results
 * look up appropriate scores from m_pSeqUtilFrag->m_pfAScore
 */
        final
        int tPos = m_tSeqPos;
        while (a > 0) {
            cValue = m_pSeq[a];
            dValue += m_pSeqUtilFrag.getAaMass(cValue, tPos + a);
            lValue = mconvert(dValue, _c);
            m_plSeq[lCount] = lValue;
            m_pfSeq[lCount] = pfScore[cValue];
            lCount++;
            a--;
        }
/*
 * set the next integer mass value to 0: this marks the end of the array 
 */
        m_plSeq[lCount] = 0;
        return true;
    }
/*
 * create list of non-zero predicted intensity values for y-ions and their
 * integer converted m/z values
 */

    boolean add_Y(final long _t, final long _c)
    {
        int a = m_lSeqLength - 1;
/*
 * get the conversion factor between a straight sequence mass and a y-ion
 */
        double dValue = m_pSeqUtilFrag.getdY(); //  - > m_dY;

        int lValue = 0;
/*
 * deal with non-hydrolytic cleavage
 */
        dValue += (m_pSeqUtilFrag.getdCleaveNdefault() - m_pSeqUtilFrag.getdCleaveCdefault()); // - > m_dCleaveCdefault);
        dValue = incrementDValue(dValue);
 /*
/*
 * deal with protein C-teminus
 */
        if (m_bIsC) {
            dValue += m_pSeqUtilFrag.getfCT(); //  - > m_fCT;
        }
        char cValue = '\0';

        int lCount = 0;
        float fSub = 0.0F;
        float[] pfScore = m_pSeqUtilFrag.getPfYScore(); // - > m_pfYScore;
        float[] pfScoreMinus = m_pSeqUtilFrag.getPfBScore(); // - > m_pfBScore;
/*
 * from C- to N-terminus, calcuate fragment ion m/z values and store the results
 * look up appropriate scores from m_pSeqUtilFrag->m_pfAScore
 */
        final
        int tPos = m_tSeqPos;
        while (a > 0) {
            cValue = m_pSeq[a];
            dValue += m_pSeqUtilFrag.getAaMass(cValue, tPos + a); // - > getAaMass(;
            lValue = mconvert(dValue, _c);
            if (_t == 0) {
                if (a < 5) {
                    m_plSeq[lCount] = lValue;
                    m_pfSeq[lCount] = pfScore[cValue] * pfScoreMinus[m_pSeq[a - 1]];
                    lCount++;
                }
            }
            else {
                m_plSeq[lCount] = lValue;
                m_pfSeq[lCount] = pfScore[cValue] * pfScoreMinus[m_pSeq[a - 1]];
                if (a == 2) {
                    if (m_pSeq[1] == 'P') {
                        m_pfSeq[lCount] *= 10;
                    }
                    else {
                        m_pfSeq[lCount] *= 3;
                    }
                }
                lCount++;
            }
            a--;
        }
/*
 * set the next integer mass value to 0: this marks the end of the array 
 */
        m_plSeq[lCount] = 0;
        return true;
    }

/*
 * create list of non-zero predicted intensity values for y-ions and their
 * integer converted m/z values
 */

    boolean add_Z(final long _t, final long _c)
    {
        int a = m_lSeqLength - 1;
/*
 * get the conversion factor between a straight sequence mass and a y-ion
 */
        double dValue = m_pSeqUtilFrag.getdZ(); // - > m_dZ;

        int lValue = 0;
/*
 * deal with non-hydrolytic cleavage
 */
        dValue += (m_pSeqUtilFrag.getdCleaveNdefault() - m_pSeqUtilFrag.getdCleaveCdefault()); //  - > m_dCleaveCdefault);
        dValue = incrementDValue(dValue);
/*
/*
 * deal with protein C-teminus
 */
        if (m_bIsC) {
            dValue += m_pSeqUtilFrag.getfCT(); //  - > m_fCT;
        }
        char cValue = '\0';

        int lCount = 0;
        float fSub = 0.0F;
        float[] pfScore = m_pSeqUtilFrag.getPfYScore(); // - > m_pfYScore;
        float[] pfScoreMinus = m_pSeqUtilFrag.getPfBScore(); //  - > m_pfBScore;
/*
 * from C- to N-terminus, calcuate fragment ion m/z values and store the results
 * look up appropriate scores from m_pSeqUtilFrag->m_pfAScore
 */
        final
        int tPos = m_tSeqPos;
        while (a > 0) {
            cValue = m_pSeq[a];
            dValue += m_pSeqUtilFrag.getAaMass(cValue, tPos + a);
            lValue = mconvert(dValue, _c);
            m_plSeq[lCount] = lValue;
            m_pfSeq[lCount] = pfScore[cValue] * pfScoreMinus[m_pSeq[a - 1]];
            lCount++;
            a--;
        }
/*
 * set the next integer mass value to 0: this marks the end of the array 
 */
        m_plSeq[lCount] = 0;
        return true;
    }

    boolean sort_details()
    {
/*
 * update the mstate object
 */
        m_State.create_equals((long) m_vSpec.size());
/*
 * sort the m_vDetails vector to improve efficiency at modeling the vector
 */
        Collections.sort(m_vDetails,new LessThanDetails());

       // sort(m_vDetails.begin(), m_vDetails.end(), LessThanDetails);
/*
 * store the mspectrumdetails object for the mspectrum
 */

        m_lDetails =   m_vDetails.size();
        int tLimit = m_vDetails.size();
        int a = 0;
        m_sIndex.clear();
        float fLast = 0.0F;
        while (a < tLimit) {

            float ix = m_vDetails.get(a).getfU();
            SpectrumIndex indTemp = new SpectrumIndex(ix, a);
            if (indTemp.getfM() != fLast) {
                m_sIndex.add(indTemp);
            }
            fLast = indTemp.getfM();
            a += 5;
        }
        return true;
    }
/*
 * add_mi does the work necessary to set up an mspectrum object for modeling.
 * default implementation simply checks for errors, and sets spectrum count.
 * override in an mscore derived class implementation to do algorithm specific
 * processing.
 */

    boolean add_mi(Spectrum _s)
    {
/*
 * the fragment ion error cannot be zero
 */
        if (m_fErr == 0.0)
            return false;

        m_lSpectra = m_vSpec.size();
        return true;
    }

/*
 * add_mi does the work necessary to set up an mspectrum object for modeling. 
 *   - a copy of the mspectrum object is added to m_vSpec
 *   - an entry in the m_State object is made for the m_ParentStream ion M+H
 * once an mspectrum has been added, the original mspectrum is no longer
 * needed for modeling, as all of the work associated with a spectrum
 * is only done once, prior to modeling sequences.
 */

    boolean add_details(Spectrum _s)
    {
/*
 * if there is a limit on the number of peaks, sort the temporary mspectrum.m_vMI member
 * using lessThanMI (see top of this file). the sort results in m_vMI having the most
 * intense peaks first in the vector. then, simply erase all but the top m_lMaxPeaks values
 * from that vector and continue.
/*
 * the fragment ion error cannot be zero
 */
        if (m_fErr == 0.0)
            return false;
/*
 * create a temporary mspec object
 */
        Spectrum spCurrent = new Spectrum();
        spCurrent = _s;
/*
 * store the mspec object
 */
        m_vSpec.add(spCurrent);
        SpectrumDetails detTemp = new SpectrumDetails();
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        if ((m_lErrorType & ScoreError.T_PARENT_PPM) != 0) {
//            detTemp.m_fL = (float) (_s.m_dMH - (_s.m_dMH * m_fParentErrPlus / 1e6));
//            detTemp.m_fU = (float) (_s.m_dMH + (_s.m_dMH * m_fParentErrMinus / 1e6));
//        }
//        else {
//            detTemp.m_fL = (float) (_s.m_dMH - m_fParentErrPlus);
//            detTemp.m_fU = (float) (_s.m_dMH + m_fParentErrMinus);
//        }
//        if (detTemp.getfU() > m_fMaxMass) {
//            m_fMaxMass = detTemp.getfU();
//        }
//        detTemp.m_lA = (long) m_vSpec.size() - 1;
//        m_vDetails.add(detTemp);
//        if (m_bIsotopeError) {
//            if (spCurrent.m_fMH > 1000.0) {
//                detTemp.m_fL -= (float) (1.00335);
//                detTemp.m_fU -= (float) (1.00335);
//                m_vDetails.push_back(detTemp);
//            }
//            if (spCurrent.m_fMH > 1500.0) {
//                detTemp.m_fL -= (float) 1.00335;
//                detTemp.m_fU -= (float) 1.00335;
//                m_vDetails.push_back(detTemp);
//            }
//        }
//        return true;
    }
/*
 * add_seq stores a sequence, if the current value of m_pSeq contains the N-terminal portion
 * of the new sequence. this method is part of optimizing the scoring process: all references
 * to it could be replaced by set_seq. set_seq must be called before add_seq is called.
 */


    long add_seq(String _s, final boolean _n, final boolean _c, final int _l,
                 final int _f)
    {
        if (_s == null)
            return 0;
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        long lOldLength = m_lSeqLength;
//        m_lSeqLength = _l;
///*
// * if the sequence is too long, use set_seq to adjust the arrays and store the sequence
// */
//        if (m_lSeqLength >= m_lSize - 1) {
//            return set_seq(_s, _n, _c, _l, _f);
//        }
///*
// * copy the new part of the sequence into m_pSeq
// */
//        strcpy(m_pSeq + lOldLength, _s + lOldLength);
//
//        int a = lOldLength;
//        m_bIsC = _c;
//        m_State.initialize(m_pSeq, m_lSize);
//        m_Term.initialize(m_seqUtil.m_pdAaMod['['], m_seqUtil.m_pdAaMod[']']);
///*
// * update the m_ParentStream ion M+H value
// */
//        map<int, int>::iterator itValue;
//        map<int, int>::iterator itEnd = m_seqUtil.m_mapMotifs.end();
//        SMap::iterator itSeq;
//        SMap::iterator itSeqEnd = m_seqUtil.m_mapMods.end();
//        if (m_seqUtil.m_bPotentialMotif) {
//            m_seqUtil.clear_motifs(false);
//        }
//        while (a < m_lSeqLength) {
//            m_dSeqMH += m_seqUtil.m_pdAaMass[m_pSeq[a]] + m_seqUtil.m_pdAaMod[m_pSeq[a]] + m_seqUtil.m_pdAaFullMod[m_pSeq[a]];
//            if (m_seqUtil.m_bSequenceMods) {
//                itSeq = m_seqUtil.m_mapMods.find((long) (m_tSeqPos + a));
//                if (itSeq != itSeqEnd) {
//                    m_dSeqMH += itSeq - > second;
//                }
//            }
//            if (m_seqUtil.m_pdAaMod[m_pSeq[a] + 32] != 0.0) {
//                m_State.add_mod(m_pSeq + a);
//            }
//            if (m_seqUtil.m_bPotentialMotif) {
//                itValue = m_seqUtil.m_mapMotifs.find(m_tSeqPos + a);
//                if (itValue != itEnd) {
//                    m_State.add_mod(m_pSeq + a);
//                    m_seqUtil.add_mod(m_pSeq[a], itValue - > second);
//                }
//            }
//            a++;
//        }
//        if (m_seqUtil.m_bPotentialMotif) {
//            m_seqUtil.set_motifs();
//        }
///*
// * deal with protein terminii
// */
//        if (m_bIsC)
//            m_dSeqMH += m_seqUtil.m_fCT;
///*
// * update the mstate object
// */
//        m_State.setdSeqMHS(m_dSeqMH);
//        m_fMinMass = (float) m_dSeqMH;
//        if (m_bUsePam) {
//            m_Pam.initialize(m_pSeq, (int) m_lSize, (float) m_dSeqMH);
//        }
//        if (m_bUseSaps) {
//            m_Sap.initialize(m_pSeq, (int) m_lSize, (float) m_dSeqMH);
//        }
//        return m_lSeqLength;
    }
/*
 * check_parents is used by the state machine for dealing with potentially
 * modified sequences. it determines how many m_ParentStream ions are eligible
 * to be generated by the current modified sequence and stores the
 * vector indices of those eligible spectra in the m_State object.
 * this test is done to improve performance. inlining is not necessary,
 * but this method is called very often in a normal protein modeling session, so removing
 * the method calling overhead can improve overall performance
 */

    boolean check_parents()
    {
/*
 * this check improves performance because of the way the state machine assigns modifications
 * if the state machine sequence order is modified, this mechanism should be reviewed
 */
        if (m_State.getdSeqMHFailedS() == m_dSeqMH) {
            m_State.setlEqualsS(0);
            return false;
        }
        if (m_dSeqMH < m_vDetails.get(0).getfL()) {
            return false;
        }
        if (m_dSeqMH > m_vDetails.get(m_lDetails - 1).getfL()) {
            return false;
        }
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        vector<mspectrumdetails>::iterator itDetails = m_vDetails.begin();
//        vector<mspectrumdetails>::iterator itEnd = m_vDetails.end();
//        float fSeqMH = (float) m_dSeqMH;
//        set<mspectrumindex>::iterator itIndex;
//        mspectrumindex indTemp;
//        indTemp.m_fM = fSeqMH;
//        if (!m_sIndex.empty()) {
//            itIndex = m_sIndex.lower_bound(indTemp);
//            if (itIndex != m_sIndex.begin()) {
//                itIndex--;
//            }
//            itDetails = itDetails + ( * itIndex).m_tA;
//        }
//        while (itDetails != itEnd) {
//            if (*itDetails == fSeqMH){
//                m_State.m_lEqualsS = 0;
//                m_State.m_plEqualsS[m_State.m_lEqualsS] = itDetails - > m_lA;
//                m_State.m_lEqualsS++;
//                itDetails++;
//                while (itDetails != itEnd &&*itDetails == fSeqMH){
//                    m_State.m_plEqualsS[m_State.m_lEqualsS] = itDetails - > m_lA;
//                    m_State.m_lEqualsS++;
//                    itDetails++;
//                }
//                return true;
//            }
//            if (fSeqMH < itDetails - > m_fL) {
//                break;
//            }
//            itDetails++;
//        }
//
///*
// * this check improves performance because of the way the state machine assigns modifications
// * if the state machine sequence order is modified, this mechanism should be reviewed
// */
//        m_State.setdSeqMHS(m_dSeqMH);
//        m_State.setlEqualsS(0);
//        return false;
    }


/*
 * get_aa is used to check the current sequence and determine how many of 
 * the residues correspond to modification sites. this method is not used
 * by mscore: check mprocess to see an example of how it is used to create
 * a list of modified residues in a domain
 */

//    boolean get_aa(List<maa> _m, final int _a, double&_d)
//    {
//        _d = 1000000.0;
//        if (!m_seqUtil.m_bComplete && !m_seqUtil.m_bPotential)
//            return false;
//        _m.clear();
//        int a = 0;
//        char cRes = '\0';
//        char*pValue = null;
//        maa aaValue;
//        double dDelta = 0.0;
//        while (a < 128) {
//            if (a == '[' && m_seqUtil.m_pdAaFullMod['['] != 0.0) {
//                cRes = m_pSeq[0];
//                aaValue.m_cRes = cRes;
//                aaValue.m_dMod = m_seqUtil.m_pdAaFullMod['['];
//                aaValue.m_lPos = (long) _a;
//                _m.push_back(aaValue);
//            }
//            if (a == ']' && m_seqUtil.m_pdAaFullMod[']'] != 0.0) {
//                cRes = m_pSeq[strlen(m_pSeq) - 1];
//                aaValue.m_cRes = cRes;
//                aaValue.m_dMod = m_seqUtil.m_pdAaFullMod[']'];
//                aaValue.m_lPos = (long) _a + (long) strlen(m_pSeq) - 1;
//                _m.push_back(aaValue);
//            }
//            if (m_seqUtil.m_pdAaMod[a] != 0.0) {
//                cRes = (char) a;
//                pValue = strchr(m_pSeq, cRes);
//                while (pValue != null) {
//                    aaValue.m_cRes = cRes;
//                    if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                        aaValue.m_cRes -= 32;
//                    }
//                    aaValue.m_dMod = m_seqUtil.m_pdAaMod[a];
//                    aaValue.m_dPrompt = m_seqUtil.m_pdAaPrompt[a];
//                    aaValue.m_lPos = (long) _a + (long) (pValue - m_pSeq);
//                    dDelta += aaValue.m_dMod;
//                    _m.push_back(aaValue);
//                    pValue++;
//                    pValue = strchr(pValue, cRes);
//                }
//            }
//            if (m_seqUtil.m_pdAaFullMod[a] != 0.0) {
//                cRes = (char) a;
//                pValue = strchr(m_pSeq, cRes);
//                while (pValue != null) {
//                    aaValue.m_cRes = cRes;
//                    if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                        aaValue.m_cRes -= 32;
//                    }
//                    aaValue.m_dMod = m_seqUtil.m_pdAaFullMod[a];
//                    aaValue.m_lPos = (long) _a + (long) (pValue - m_pSeq);
//                    aaValue.m_dPrompt = 0.0;
//                    _m.push_back(aaValue);
//                    pValue++;
//                    pValue = strchr(pValue, cRes);
//                }
//            }
//            a++;
//        }
//        if (m_seqUtil.m_bSequenceMods) {
//            SMap::iterator itValue = m_seqUtil.m_mapMods.begin();
//            SMap::final_iterator itEnd = m_seqUtil.m_mapMods.end();
//            int tValue = 0;
//            int tEnd = m_tSeqPos + m_lSeqLength;
//            while (itValue != itEnd) {
//                tValue = itValue - > first;
//                if (tValue >= m_tSeqPos || tValue < tEnd) {
//                    tValue = tValue - m_tSeqPos;
//                    cRes = m_pSeq[tValue];
//                    aaValue.m_cRes = cRes;
//                    if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                        aaValue.m_cRes -= 32;
//                    }
//                    aaValue.m_dMod = itValue - > second;
//                    aaValue.m_lPos = (long) (_a + tValue);
//                    if (cRes <= 'Z') {
//                        cRes += 32;
//                    }
//                    dDelta += aaValue.m_dMod;
//                    aaValue.m_dPrompt = m_seqUtil.m_pdAaPrompt[cRes];
//                    _m.push_back(aaValue);
//                }
//                itValue++;
//            }
//        }
//        if (m_Term.getlN() != 0) {
//            aaValue.m_dMod = m_seqUtil.m_pdAaMod['['];
//            aaValue.m_dPrompt = 0.0;
//            aaValue.m_lPos = (long) _a;
//            aaValue.m_cRes = m_pSeq[0];
//            if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                aaValue.m_cRes -= 32;
//            }
//            dDelta += aaValue.m_dMod;
//            _m.push_back(aaValue);
//        }
//        if (m_Term.getlN() != 0) {
//            aaValue.m_dMod = m_seqUtil.m_pdAaMod[']'];
//            aaValue.m_dPrompt = 0.0;
//            aaValue.m_lPos = (long) _a + m_lSeqLength - 1;
//            aaValue.m_cRes = m_pSeq[m_lSeqLength - 1];
//            if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                aaValue.m_cRes -= 32;
//            }
//            dDelta += aaValue.m_dMod;
//            _m.push_back(aaValue);
//        }
//        if (m_Pam.m_tCount > 0) {
//            aaValue.m_dMod = m_seqUtil.m_pdAaMass[m_pSeq[m_Pam.m_tPos]] - m_seqUtil.m_pdAaMass[m_Pam.m_pSeqTrue[m_Pam.m_tPos]];
//            aaValue.m_dPrompt = m_seqUtil.m_pdAaPrompt[m_pSeq[m_Pam.m_tPos]] - m_seqUtil.m_pdAaPrompt[m_Pam.m_pSeqTrue[m_Pam.m_tPos]];
//            aaValue.m_lPos = (long) (_a + m_Pam.m_tPos);
//            aaValue.m_cRes = m_Pam.m_pSeqTrue[m_Pam.m_tPos];
//            if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                aaValue.m_cRes -= 32;
//            }
//            aaValue.m_cMut = m_pSeq[m_Pam.m_tPos];
//            aaValue.m_strId.clear();
//            dDelta += aaValue.m_dMod;
//            _d = dDelta;
//            _m.push_back(aaValue);
//        }
//        if (m_Sap.m_tCount > 0) {
//            aaValue.m_dMod = m_seqUtil.m_pdAaMass[m_pSeq[m_Sap.m_tPos]] - m_seqUtil.m_pdAaMass[m_Sap.m_pSeqTrue[m_Sap.m_tPos]];
//            aaValue.m_dPrompt = m_seqUtil.m_pdAaPrompt[m_pSeq[m_Sap.m_tPos]] - m_seqUtil.m_pdAaPrompt[m_Sap.m_pSeqTrue[m_Sap.m_tPos]];
//            aaValue.m_lPos = (long) (_a + m_Sap.m_tPos);
//            aaValue.m_cRes = m_Sap.m_pSeqTrue[m_Sap.m_tPos];
//            if (aaValue.m_cRes >= 'a' && aaValue.m_cRes <= 'z') {
//                aaValue.m_cRes -= 32;
//            }
//            aaValue.m_cMut = m_pSeq[m_Sap.m_tPos];
//            aaValue.m_strId = m_Sap.m_strId;
//            if (aaValue.m_cMut >= 'a' && aaValue.m_cMut <= 'z') {
//                aaValue.m_cMut -= 32;
//            }
//            dDelta += aaValue.m_dMod;
//            _d = dDelta;
//            _m.push_back(aaValue);
//        }
//        return true;
//    }
/*
 * mconvert converts from mass and charge to integer ion value
 * for mi vector.
 */


    int mconvert(double _m, final long _c)
    {
/*
 * calculate the conversion factor between an m/z value and its integer value
 * as referenced in m_vsmapMI
 */
        final double dZ = (double) _c;
        return (int) ((m_pSeqUtilFrag.getdProton() + _m / dZ) * m_fWidth / m_fErr);
    }
/*
 * hfactor returns a factor applied to the score to produce the
 * hyper score, given a number of ions matched.
 */

    double hfactor(long _l)
    {
        return 1.0;
    }
/*
 * sfactor returns a factor applied to the final convolution score.
 */

    double sfactor()
    {
        return 1.0;
    }
/*
 * hconvert is use to convert a hyper score to the value used in a score
 * histogram, since the actual scoring distribution may not lend itself
 * to statistical analysis through use of a histogram.
 */

    float hconvert(float _f)
    {
        if (_f <= 0.0)
            return 0.0F;
        return (float) (m_dScale * _f);
    }
/*
 * report_score formats a hyper score for output.
 */

    void report_score(StringBuilder buffer, float hyperscore)
    {
        final float v = hconvert(hyperscore);

        buffer.append(Float.toString(v));
      //  sprintf(buffer, "%.1f", v);
    }

/*
 * load_next is used access the potential sequence modification state machine.
 * it runs the state machine until either:
 *   - a modified sequence that has a mass that is within error of 
 *     one of the mspectrum objects has been found, or
 *   - the last of the modified sequences has been returned
 * it returns true if there are more sequences to consider.
 * NOTE: the last sequence generated by the state machine is the one without any
 *       of the potential modifications in place. maintaining this order is important
 *       to the correct functioning of the logic using in mprocess.
 */

    boolean load_next()
    {
        boolean bReturn = false;
        if (!(m_bUsePam || m_bUseSaps)) {
            bReturn = load_state();
            if (bReturn) {
                return bReturn;
            }
            bReturn = load_next_term();
            return bReturn;
        }
        else if (m_bUsePam) {
// Shift mutation to be checked
            if (m_Pam.gettCount() != 0) {
                bReturn = load_state();
                if (bReturn) {
                    return bReturn;
                }
                bReturn = load_next_term();
                if (bReturn) {
                    return bReturn;
                }
            }
            bReturn = load_next_pam();
            return bReturn;
        }
        else if (m_bUseSaps) {
            bReturn = load_state();
            if (bReturn) {
                return bReturn;
            }
            bReturn = load_next_term();
            if (bReturn) {
                return bReturn;
            }
            if (m_Sap.isbOk()) {
                bReturn = load_next_sap();
            }
            return bReturn;
        }
        return bReturn;
    }
/*
 * as of version 2004.03.01, a new state machine has been added that allows for testing
 * for N- and C- terminal modifications as partial modifications. Previous versions
 * tested for these modifications as complete modifications only. This state machine
 * relies on the mscoreterm class, m_Term, to maintain information about the current modification
 * state of the N- and C- terminus of the peptide. See the source for that class (in mscorestate.h)
 * for an explanation of how the state is stored and referenced.
 */

    boolean load_next_term()
    {
        if (!m_Term.isbC() && !m_Term.isbN()) {
            return false;
        }
        throw new UnsupportedOperationException("Fix This"); // ToDo
//// reset the state machine, as there are no further states
//        if (m_Term.getlState() == 3) {
//            m_Term.initialize(m_seqUtil.m_pdAaMod['['], m_seqUtil.m_pdAaMod[']']);
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMod['['];
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMod[']'];
//            m_State.initialize(m_pSeq, m_lSeqLength);
//            m_State.setdSeqMHS(m_dSeqMH);
//            m_fMinMass = (float) m_dSeqMH;
//            m_State.setlEqualsS(0);
//            check_parents();
//            return false;
//        }
//        boolean bReturn = false;
//        if (m_Term.getlState() == 0) {
//// deal with modification at N-terminus, if possible
//            if (m_Term.isbN()) {
//                m_Term.m_lState = 1;
//                m_dSeqMH += m_pSeqUtilFrag - > m_pdAaMod['['];
//                m_Term.getlN() != 0 = 1;
//                m_Term.getlN() != 0 = 0;
//                m_State.initialize(m_pSeq, m_lSeqLength);
//                m_State.setdSeqMHS(m_dSeqMH);
//                m_fMinMass = (float) m_dSeqMH;
//                m_State.setlEqualsS(0);
//                check_parents();
//                return true;
//            }
//// deal with modification at C-terminus, if possible
//            if (m_Term.isbC()) {
//                m_Term.setlState(2);
//                m_dSeqMH += m_pSeqUtilFrag - > m_pdAaMod[']'];
//                m_Term.getlN() != 0 = 0;
//                m_Term.getlN() != 0 = 1;
//                m_State.initialize(m_pSeq, m_lSeqLength);
//                m_State.setdSeqMHS(m_dSeqMH);
//                m_fMinMass = (float) m_dSeqMH;
//                m_State.setlEqualsS(0);
//                check_parents();
//                return true;
//            }
//            return false;
//        }
//// deal with modification at C-terminus, if the N-terminus is in the modified state
//        else if (m_Term.getlState() == 1) {
//            if (!m_Term.isbC()) {
//                m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMod['['];
//                m_Term.initialize(m_seqUtil.m_pdAaMod['['], m_seqUtil.m_pdAaMod[']']);
//                return false;
//            }
//            m_Term.getlN() != 0 = 0;
//            m_Term.getlN() != 0 = 1;
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMod['['];
//            m_dSeqMH += m_pSeqUtilFrag - > m_pdAaMod[']'];
//            m_Term.m_lState = 2;
//            m_State.initialize(m_pSeq, m_lSeqLength);
//            m_State.m_dSeqMHS = m_dSeqMH;
//            m_fMinMass = (float) m_dSeqMH;
//            m_State.m_lEqualsS = 0;
//            check_parents();
//            return true;
//        }
//// deal with modification at both N- and C- terminus, if possible
//        else if (m_Term.m_lState == 2) {
//            if (!m_Term.m_bN) {
//                m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMod[']'];
//                m_Term.initialize(m_pSeqUtilFrag - > m_pdAaMod['['],
//                        m_pSeqUtilFrag - > m_pdAaMod[']']);
//                return false;
//            }
//            m_dSeqMH += m_pSeqUtilFrag - > m_pdAaMod['['];
//            m_Term.getlN() != 0 = 1;
//            m_Term.getlN() != 0 = 1;
//            m_State.initialize(m_pSeq, m_lSeqLength);
//            m_State.m_dSeqMHS = m_dSeqMH;
//            m_fMinMass = (float) m_dSeqMH;
//            m_Term.m_lState = 3;
//            m_State.m_lEqualsS = 0;
//            check_parents();
//        }
//        return bReturn;
    }
/*
 * load_next_pam alters the current peptide sequence by iterating through all possible
 * single amino acid polymorphisms. Polymorphisms that are difficult to distinguish by
 * mass spectrometry are skipped to avoid reporting spurious assignments.
 */

    boolean load_next_pam()
    {
        boolean bReturn = false;
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        if (m_Pam.m_tCount != 0) {
//            m_Pam.m_tAa++;
//        }
//        m_Pam.m_tCount++;
//// Shift to next residue if all possibilities have been checked
//        if (m_Pam.m_tAa >= m_Pam.m_tAaTotal) {
//            m_pSeq[m_Pam.m_tPos] = m_Pam.m_pSeqTrue[m_Pam.m_tPos];
//            m_Pam.m_tPos++;
//            m_Pam.m_tAa = 0;
//        }
//// Skip mutations indistinguishable from the sequence
//        while (m_Pam.m_tPos < m_Pam.m_tLength && check_pam_mass()) {
//            if (m_Pam.m_tAa == m_Pam.m_tAaTotal - 1) {
//                m_pSeq[m_Pam.m_tPos] = m_Pam.m_pSeqTrue[m_Pam.m_tPos];
//                m_Pam.m_tPos++;
//                m_Pam.m_tAa = 0;
//            }
//            else {
//                m_Pam.m_tAa++;
//            }
//        }
//// Return false if all residues have been checked
//        if (m_Pam.m_tPos >= m_Pam.m_tLength) {
//            strcpy(m_pSeq, m_Pam.m_pSeqTrue);
//            m_dSeqMH = m_Pam.m_fSeqTrue;
//            m_State.m_dSeqMHS = m_dSeqMH;
//            m_fMinMass = (float) m_dSeqMH;
//            m_State.m_lEqualsS = 0;
//            check_parents();
//            m_Pam.m_tCount = 0;
//            return false;
//        }
//        else {
//            strcpy(m_pSeq, m_Pam.m_pSeqTrue);
//            m_dSeqMH = m_Pam.m_fSeqTrue;
//            m_dSeqMH += m_pSeqUtilFrag - > m_pdAaMass[m_Pam.m_pAa[m_Pam.m_tAa]];
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMass[m_Pam.m_pSeqTrue[m_Pam.m_tPos]];
//            m_dSeqMH += m_pSeqUtilFrag - > m_pdAaFullMod[m_Pam.m_pAa[m_Pam.m_tAa]];
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaFullMod[m_Pam.m_pSeqTrue[m_Pam.m_tPos]];
//            m_pSeq[m_Pam.m_tPos] = m_Pam.m_pAa[m_Pam.m_tAa];
//            m_State.initialize(m_pSeq, m_lSeqLength);
//            m_State.m_dSeqMHS = m_dSeqMH;
//            m_fMinMass = (float) m_dSeqMH;
//            m_State.m_lEqualsS = 0;
//            check_parents();
//            return true;
//        }
//        return bReturn;
    }
/*
 * this method checks for modifications that result from trivial changes in mass assignment, e.g.
 * M->F (+16), even though M+16 is being checked a potential modification. getting this function
 * right is critical to the point mutation assignment working properly.
 */

    boolean check_pam_mass()
    {
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        final char cTrue = m_Pam.m_pSeqTrue[m_Pam.m_tPos];
//        final char cNew = m_Pam.m_pAa[m_Pam.m_tAa];
//        final float fTrue = m_pSeqUtilFrag - > m_pfAaMass[cTrue] + (float) m_pSeqUtilFrag - > m_pdAaFullMod[cTrue];
//        final float fNew = m_pSeqUtilFrag - > m_pfAaMass[cNew] + (float) m_pSeqUtilFrag - > m_pdAaFullMod[cNew];
//        if (fabs(fTrue - fNew) < m_fHomoError) {
//            return true;
//        }
//        if (fabs(fTrue + m_pSeqUtilFrag - > m_pdAaMod[cTrue + 32] - fNew) < m_fHomoError) {
//            return true;
//        }
//        if (fabs(fNew + m_pSeqUtilFrag - > m_pdAaMod[cNew + 32] - fTrue) < m_fHomoError) {
//            return true;
//        }
//        if (fabs(fTrue + m_pSeqUtilFrag - > m_pdAaMod[cTrue + 32] - fNew) < m_fHomoError) {
//            return true;
//        }
//        if (fabs(
//                fNew + m_pSeqUtilFrag - > m_pdAaMod[cNew + 32] - fTrue - m_pSeqUtilFrag - > m_pdAaMod[cTrue + 32]) < m_fHomoError) {
//            return true;
//        }
//        return false;
    }

/*
 * load_next_sap alters the current peptide sequence by adding single amino acid
 * polymorphisms that have been annotated in the SAP files specified in the taxonomy
 * file settings.
 */

    boolean load_next_sap()
    {
        boolean bReturn = false;
        if (!m_Sap.isbOk()) {
            return bReturn;
        }
        throw new UnsupportedOperationException("Fix This"); // ToDo
//// Return false if all residues have been checked
//        if (!m_Sap.next()) {
//            strcpy(m_pSeq, m_Sap.m_pSeqTrue);
//            m_dSeqMH = m_Sap.m_fSeqTrue;
//            m_State.m_dSeqMHS = m_dSeqMH;
//            m_fMinMass = (float) m_dSeqMH;
//            m_State.m_lEqualsS = 0;
//            check_parents();
//            m_Sap.m_tCount = 0;
//            return false;
//        }
//        else {
//            strcpy(m_pSeq, m_Sap.m_pSeqTrue);
//            m_dSeqMH = m_Sap.m_fSeqTrue;
//            m_dSeqMH += m_pSeqUtilFrag - > m_pdAaMass[m_Sap.m_cCurrent];
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaMass[m_Sap.m_pSeqTrue[m_Sap.m_tPos]];
//            m_dSeqMH += m_pSeqUtilFrag - > m_pdAaFullMod[m_Sap.m_cCurrent];
//            m_dSeqMH -= m_pSeqUtilFrag - > m_pdAaFullMod[m_Sap.m_pSeqTrue[m_Sap.m_tPos]];
//            m_pSeq[m_Sap.m_tPos] = m_Sap.m_cCurrent;
//            m_State.initialize(m_pSeq, m_lSeqLength);
//            m_State.m_dSeqMHS = m_dSeqMH;
//            m_fMinMass = (float) m_dSeqMH;
//            m_State.m_lEqualsS = 0;
//            check_parents();
//            return true;
//        }
//        return bReturn;
    }
/*
 * load_state is used access the potential sequence modification state machine.
 * it runs the state machine until either:
 *   - a modified sequence that has a mass that is within error of 
 *     one of the mspectrum objects has been found, or
 *   - the last of the modified sequences has been returned
 * it returns true if there are more sequences to consider.
 * NOTE: the last sequence generated by the state machine is the one without any
 *       of the potential modifications in place. maintaining this order is important
 *       to the correct functioning of the logic using in mprocess.
 */

    boolean load_state()
    {
        boolean bReturn = run_state_machine();
        if (m_dSeqMH < m_fMinMass) {
            m_fMinMass = (float) m_dSeqMH;
        }
        while (bReturn) {
            if (check_parents()) {
                return bReturn;
            }
            bReturn = run_state_machine();
            if (m_dSeqMH < m_fMinMass) {
                m_fMinMass = (float) m_dSeqMH;
            }
        }
        return bReturn;
    }
/*
 * load_seq is used to call the appropriate add_? method to create the
 * ion type specific scoring array. These arrays are padded to mod(4), so
 * that an optimizing compiler can utilize MMX arrays in the dot method.
 */

    boolean load_seq(final long _t, final long _c)
    {
        boolean bReturn = true;
        if ((ScoreType.T_Y & _t) != 0) {
            bReturn = add_Y(_t, _c);
        }
        else if ((ScoreType.T_X & _t) != 0) {
            bReturn = add_X(_t, _c);
        }
        else if ((ScoreType.T_A & _t) != 0) {
            bReturn = add_A(_t, _c);
        }
        else if ((ScoreType.T_B & _t) != 0) {
            bReturn = add_B(_t, _c);
        }
        else if ((ScoreType.T_C & _t) != 0) {
            bReturn = add_C(_t, _c);
        }
        else if ((ScoreType.T_Z & _t) != 0) {
            bReturn = add_Z(_t, _c);
        }
        return bReturn;
    }

/*
 * modifications have exponential impact on number of sequences that require scoring,
 * so a limit is imposed to keep things like a modification on M, and a sequence with
 * 30 M's from costing too much.
 */

   public static final int getMaxModStates() {
       return 1<<12;
   }
  
/*
 * the state machine for determining all potentially modified residues in a peptide sequence
 * and then generating and scoring them efficiently relies upon run_state_machine.
 *
 * this version calculates the various combinations in order of the number of actual
 * modifications present.  for example, if there are 10 possible modification sites,
 * then all combinations with 1 modification are checked first, then all combinations
 * with 2 modifications, with 3, with 4, etc.
 *
 * this makes it possible to impose a maximum number of states checked, while still
 * checking the most likely combinations first.  i.e. checking all possible combinations
 * of 1-10 modifications on 30 sites is probably better than all combinations of the
 * first 10 sites, as with FIX1.
 */

    boolean run_state_machine()
    {
        throw new UnsupportedOperationException("Fix This"); // ToDo
/*
 * return false if there are no more states
 */
//        if (!m_State.isbStateS()) {
//            strcpy(m_pSeq, m_State.m_pSeqS);
//            m_dSeqMH = m_State.m_dSeqMHS;
//            return false;
//        }
//        else if (m_State.getlStates() >= mscorestate::M_lMaxModStates)
//        {
//            strcpy(m_pSeq, m_State.m_pSeqS);
//            m_dSeqMH = m_State.m_dSeqMHS;
//            m_State.m_bStateS = false;
//            m_State.m_lStates++;
//            return true;
//        }
//        m_State.m_lStates++;
//        boolean bReturn = m_State.m_bStateS;
///*
// * deal efficiently with protein modeling sessions that do not require potential modifications
// */
//        if (!m_seqUtil.m_bPotential) {
//            m_State.m_bStateS = false;
//            return bReturn;
//        }
//        if (m_State.m_lLastS == 0) {
//            m_State.m_bStateS = false;
//            return bReturn;
//        }
///*
// * adjust the states of the modification positions
// */
//        if (m_State.m_lFilledS > 0 &&
//                m_State.m_piMods[m_State.m_lCursorS] < m_State.m_lLastS - m_State.m_lFilledS + m_State.m_lCursorS) {
//            /*
//            * shift current mod through all valid positions.
//            */
//            m_State.m_piMods[m_State.m_lCursorS]++;
//        }
//        else if (m_State.m_lCursorS > 0) {
//            /*
//            * shift the mod to the left of the current mod.
//            */
//            m_State.m_lCursorS--;
//            m_State.m_piMods[m_State.m_lCursorS]++;
//            /*
//            * if there it is not at its largest possible position, shift all mods to the
//            * right back, and start over with them.
//            */
//            if (m_State.m_piMods[m_State.m_lCursorS] < m_State.m_lLastS - m_State.m_lFilledS + m_State.m_lCursorS) {
//                for (
//                        long i = 1;
//                        i < m_State.m_lFilledS
//                                - m_State.m_lCursorS;
//                        i++)
//                    m_State.m_piMods[m_State.m_lCursorS + i] = m_State.m_piMods[m_State.m_lCursorS] + i;
//                m_State.m_lCursorS = m_State.m_lFilledS - 1;
//            }
//        }
//        else if (m_State.m_lFilledS < m_State.m_lLastS) {
//            /*
//            * introduce more possible modifications, and start over calculating all
//            * possible combinations.
//            */
//            m_State.m_lFilledS++;
//            if (m_State.m_lFilledS < m_State.m_lLastS)
//                m_State.m_lCursorS = m_State.m_lFilledS - 1;
//            for (
//                    long i = 0;
//                    i < m_State.m_lFilledS
//                    ;
//                    i++)
//                m_State.m_piMods[i] = i;
//        }
//        else {
//            /*
//            * last state is no modifications.
//            */
//            m_State.m_lFilledS = 0;
//        }
///*
// * reset the sequence String and initial mass
// */
//        strcpy(m_pSeq, m_State.m_pSeqS);
//        m_dSeqMH = m_State.m_dSeqMHS;
///*
// * unmodified is the last state
// */
//        if (m_State.m_lFilledS == 0)
//            m_State.m_bStateS = false;
///*
// * otherwise, use the modification state indices to correctly set the modifications
// * in the sequence String, and add to the m_ParentStream mass.
// */
//        else {
//            for (
//                    long i = 0;
//                    i < m_State.m_lFilledS
//                    ;
//                    i++) {
//
//                long pos = m_State.m_piMods[i];
//                *(m_State.m_ppModsS[pos]) += 32;
//                m_dSeqMH += m_seqUtil.m_pdAaMod[ * (m_State.m_ppModsS[pos])];
//            }
//        }
//        return true;
    }

/*
 * the score method is called externally to score a loaded peptide against one of the
 * loaded mass spectra. the mass spectrum is refered to by its index number in the
 * m_vSpec mspectrum vector. the sequence has already been loaded via set_seq and/or
 * add_seq.
 */

    float score(final int _i)
    {
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        m_fScore = -1.0F;
//        m_fHyper = -1.0F;
//        double dFactor = 1.0;
///*
// * return -1000.0 if there is no sequence available
// */
//        if (m_pSeq == null)
//            return -1000.0F;
///*
// * initialize values for the protein modeling session
// */
//        prescore(_i);
//
//        double dScore = (float) 0.0;
//        double dValue = (float) 0.0;
//
//
//        long lType = ScoreType.T_Y;
//
//        int lValue = 0;
//
//        int lValueTotal = 0;
//
//        long lS = ScoreType.S_Y;
//
//        long lChargeLimit = (long) m_vSpec[m_lId].m_fZ;
//        // 2006.12.01: to reduce problems with high charge states, the hyperscore is
//        // calculated on the 2 best fragment ion charge states. in the
//        // previous versions the hyperscore used all available charge states, giving
//        // a considerable advantage to highly charged m_ParentStream ions
//        vector<double> vFactor;
//        int a = 0;
//        while (a < lChargeLimit + 1) {
//            vFactor.push_back(1.0);
//            a++;
//        }
//        if (lChargeLimit == 1) {
//            lChargeLimit = 2;
//        }
//        if ((m_lType & ScoreType.T_C) != 0 || (m_lType & ScoreType.T_Z) != 0) {
//            if (lChargeLimit > 2) {
//                lChargeLimit--;
//            }
//        }
///*
// * iterate through all of the possible values of the mscore_type enum
// * comparing them against m_lType
// */
//        while (lType < m_lType + 1) {
//            lValueTotal = 0;
//            dValue = 0.0;
//            if ((lType & m_lType) != 0) {
//                a = 1;
//                while (a < lChargeLimit) {
///*
// * load the sequence arrays for each possible charge states for the selected spectrum
// */
//                    load_seq(lType, a);
//                    lValue = 0;
///*
// * perform a dot product on each charge state
// */
//                    dValue += dot( & lValue);
//                    if (a == 1 && T_Y & lType && (long) m_vSpec[m_lId].m_fZ == 2) {
//
//                        long lTemp = 0;
//                        add_Y(0, 2);
//                        dValue += dot( & lTemp);
//                        lValue += lTemp;
//                    }
//                    lValueTotal += lValue;
//                    vFactor[a] *= hfactor(lValue);
//                    a++;
//                }
//                dScore += dValue;
//            }
//            m_pfScore[lS] = (float) dValue;
//            m_plCount[lS] = lValueTotal;
///*
// * move on to next value in the mstate_type enum
// */
//            lS++;
//            lType *= 2;
//        }
//        dScore *= sfactor();
//        m_fScore = (float) dScore;
//        // only use the 2 best component hyperscores
//        sort(vFactor.begin(), vFactor.end());
//        a = (long) vFactor.size() - 1;
//        dFactor = vFactor[a] * vFactor[a - 1];
//        dFactor *= dScore;
//        if (dFactor > FLT_MAX) {
//            m_fHyper = FLT_MAX;
//        }
//        else {
//            m_fHyper = (float) dFactor;
//        }
///*
// * returning 1.0 for a zero score makes the logic in mprocess easier. see mprocess:create_score
// * to see why.
// */
//        if (dScore == 0.0) {
//            dScore = 1.0;
//        }
//        return (float) dScore;
    }
/*
 * get the current value of the m_fSeqMH member variable
 */

    double seq_mh()
    {
        return m_dSeqMH;
    }
/*
 * set true if sequence is to be checked for all possible point mutations
 */

    boolean set_pam(final boolean _b)
    {
        m_bUsePam = _b;
        m_Pam.settCount(0);
        return m_bUsePam;
    }
/*
 * set true if sequence is to be checked for known single amino acid polymorphisms
 */

    boolean set_saps(final boolean _b, String _s)
    {
        m_bUseSaps = _b;
        m_Sap.set(_s, _b);
        return m_bUseSaps;
    }
/*
 * set the absolute, zero-based start position of the peptide in the full
 * protein sequence
 */

    boolean set_pos(final int _t)
    {
        m_tSeqPos = _t;
        return true;
    }
/*
 * set a new peptide sequence for consideration
 */


    long set_seq(String _s, final boolean _n, final boolean _c, final int _l,
                 final int _f)
    {
        if (_s == null)
            return 0;
        throw new UnsupportedOperationException("Fix This"); // ToDo
///*
// * adjust arrays if the sequence is longer that m_lSize
// */
//        m_lSeqLength = _l;
//        if (m_lSeqLength >= m_lSize - 1) {
//            m_lSize = m_lSeqLength + 16;
//            m_pfSeq = new float[m_lSize];
//            m_pSeq = new char[m_lSize];
//            m_plSeq = new int[m_lSize];
//        }
///*
// * make a copy of the sequence
// */
//        strcpy(m_pSeq, _s);
//        m_dSeqMH = 0.0;
//
//        int a = 0;
//        m_bIsC = _c;
//        m_bIsN = _n;
//        m_State.initialize(m_pSeq, m_lSize);
//        m_Term.initialize(m_seqUtil.m_pdAaMod['['], m_seqUtil.m_pdAaMod[']']);
//        m_State.m_lLastS = 0;
//        map<int, int>::iterator itValue;
//        map<int, int>::iterator itEnd = m_seqUtil.m_mapMotifs.end();
//        SMap::iterator itSeq;
//        SMap::iterator itSeqEnd = m_seqUtil.m_mapMods.end();
//        a = 0;
//        if (m_seqUtil.m_bPotentialMotif) {
//            m_seqUtil.clear_motifs(true);
//        }
///*
// * calculate the M+H of the sequence
// */
//        m_iCharge = 1;
//        while (a < m_lSeqLength) {
//            m_dSeqMH += m_seqUtil.m_pdAaMass[m_pSeq[a]] + m_seqUtil.m_pdAaMod[m_pSeq[a]] + m_seqUtil.m_pdAaFullMod[m_pSeq[a]];
//            if (m_seqUtil.m_bSequenceMods) {
//                itSeq = m_seqUtil.m_mapMods.find((long) (m_tSeqPos + a));
//                if (itSeq != itSeqEnd) {
//                    m_dSeqMH += itSeq - > second;
//                }
//            }
//            if (m_seqUtil.m_pdAaMod[m_pSeq[a] + 32] != 0.0)
//                m_State.add_mod(m_pSeq + a);
//            // itValue = m_seqUtil.m_mapMotifs.find(m_tSeqPos+a); // doesn't seem to do anything bpratt
//            if (m_seqUtil.m_bPotentialMotif) {
//                itValue = m_seqUtil.m_mapMotifs.find(m_tSeqPos + a);
//                if (itValue != itEnd) {
//                    m_State.add_mod(m_pSeq + a);
//                    m_seqUtil.add_mod(m_pSeq[a], itValue - > second);
//                }
//            }
//            a++;
//        }
//        if (m_seqUtil.m_bPotentialMotif) {
//            m_seqUtil.set_motifs();
//        }
//        m_dSeqMH += m_seqUtil.m_dProton + m_seqUtil.m_dCleaveN + m_seqUtil.m_dCleaveC;
//        if (m_Term.getlN() != 0) {
//            m_dSeqMH += m_seqUtil.m_pdAaMod['['];
//        }
//        if (m_Term.getlN() != 0) {
//            m_dSeqMH += m_seqUtil.m_pdAaMod[']'];
//        }
///*
// * deal with protein terminii
// */
//        if (m_bIsC)
//            m_dSeqMH += m_seqUtil.m_fCT;
//        if (m_bIsN)
//            m_dSeqMH += m_seqUtil.m_fNT;
//        m_dSeqMH += m_seqUtil.m_pdAaFullMod['['];
//        m_dSeqMH += m_seqUtil.m_pdAaFullMod[']'];
//
///*
// * record the M+H value in the mstate object
// */
//        m_State.m_dSeqMHS = m_dSeqMH;
//        m_fMinMass = (float) m_dSeqMH;
//        if (m_bUsePam) {
//            m_Pam.initialize(m_pSeq, m_lSize, (float) m_dSeqMH);
//        }
//        if (m_bUseSaps) {
//            m_Sap.initialize(m_pSeq, (int) m_lSize, (float) m_dSeqMH, _f);
//        }
//        return m_lSeqLength;
    }
/*
 * sets the types of ions (Biemann notation) to be used in scoring a peptide sequence
 */


    long set_type(final int _t)
    {
        m_lType = _t;
        return m_lType;
    }
/*
 * sets the interpretation of the m_ParentStream ion and fragment ion errors to be either absolute
 * in Daltons, or relative, in parts-per-million. the allowed values are  finalructed
 * from the values in the enum mscore_error.
 */


    long set_error(final int _t)
    {
        m_lErrorType = _t;
        return m_lErrorType;
    }
/*
 * sets the interpretation of the m_ParentStream ion and fragment ion errors to be either absolute
 * in Daltons, or relative, in parts-per-million. the allowed values are  finalructed
 * from the values in the enum mscore_error.
 */

    float set_homo_error(final float _f)
    {
        m_fHomoError = _f;
        return m_fHomoError;
    }
/*
 * sets the value for the fragment error member value m_fErr. this value is interpreted in two ways:
 * CASE 1: m_lErrorType & T_FRAGMENT_DALTONS is true
 *         in this case, the error is the error in Daltons
 * CASE 2: m_lErrorType & T_FRAGMENT_PPM is true
 *         in this case, the error in Daltons is calculated at m/z = 200.0
 *         this value is used in the blur() method and the width of the
 *         blurred distribution is scaled from the value at m/z = 200.0
 */

    float set_fragment_error(final float _f)
    {
        if (_f <= 0.0)
            return 0.0F;
        m_fErr = _f;
        if ((m_lErrorType & ScoreError.T_FRAGMENT_PPM) != 0) {
            m_fErr = (float) (200.0 * m_fErr / 1e6);
        }
/*
 * NOTE: the m_fErr value used in the ppm case is: 200 x (the error in ppm)/1000000
 */
        return m_fErr;
    }
/*
 * sets the value for the m_ParentStream error member values m_fParentErrPlus and m_fParentErrMinus.
 */

    float set_parent_error(final float _f, final boolean _b)
    {
        if (_b) {
            if (_f < 0.0)
                m_fParentErrPlus = 0.0F;
            else
                m_fParentErrPlus = _f;
        }
        else {
            if (_f < 0.0)
                m_fParentErrPlus = 0.0F;
            else
                m_fParentErrMinus = _f;
        }
        return _f;
    }
/*
 * sets the value for the m_bIsotopeError member, which checks for isotope peak assignment errors
 */

    boolean set_isotope_error(final boolean _b)
    {
        m_bIsotopeError = _b;
        return m_bIsotopeError;
    }

/*
 * sets the mass type for fragment ions
 */

    void set_fragment_masstype(MassType _t)
    {
        if (_t == MassType.average) {
            m_seqUtilAvg.set_modified(true);
            m_pSeqUtilFrag = m_seqUtilAvg;
        }
        else {
            m_pSeqUtilFrag = m_seqUtil;
        }
    }

/*
 * return true if any m_ParentStream ion is within error of the current peptide's modified M+H
 * and return the number of spectra that have M+H values greater than or within error
 * of the current peptide's modified M+H
 */

    boolean test_parents(int[]_t)
    {
        int a = 0;
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        while (a < m_lSpectra) {
//            if (m_vDetails.get(a)  == (float) m_dSeqMH) {
//                _t[0] = m_lSpectra - a;
//                return true;
//            }
//            a++;
//        }
//        return false;
    }

    boolean reset_permute()
    {
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        m_psPermute.m_tPos = 0;
//        m_psPermute.m_tEnd = m_lSeqLength - 2;
//        if (m_lSeqLength > m_psPermute.m_lSize) {
//            delete m_psPermute.m_pPerm;
//            delete m_psPermute.m_pSeq;
//            m_psPermute.m_lSize = m_lSeqLength + 16;
//            m_psPermute.m_pPerm = new char[m_psPermute.m_lSize + 1];
//            m_psPermute.m_pSeq = new char[m_psPermute.m_lSize + 1];
//        }
//        strcpy(m_psPermute.m_pSeq, m_pSeq);
//        m_psPermute.m_bRev = true;
//        return true;
    }

    boolean permute()
    {
        throw new UnsupportedOperationException("Fix This"); // ToDo
//        if (m_psPermute.m_tPos == m_psPermute.m_tEnd && m_psPermute.m_bRev) {
//            strcpy(m_pSeq, m_psPermute.m_pSeq);
//            String strTemp;
//            String strSeq = m_pSeq;
//            String::reverse_iterator itS = strSeq.rbegin();
//            String::reverse_iterator itE = strSeq.rend();
//            while (itS != itE) {
//                strTemp +=*itS;
//                itS++;
//            }
//            strcpy(m_pSeq, strTemp.c_str());
//            m_psPermute.m_bRev = false;
//            m_psPermute.m_tPos = 0;
//        }
//        if (m_psPermute.m_tPos == m_psPermute.m_tEnd) {
//            strcpy(m_pSeq, m_psPermute.m_pSeq);
//            return false;
//        }
//        memcpy(m_psPermute.m_pPerm + 1, m_pSeq, m_lSeqLength);
//        m_psPermute.m_pPerm[0] = m_psPermute.m_pPerm[m_lSeqLength];
//        m_psPermute.m_pPerm[m_lSeqLength] = '\0';
//        memcpy(m_pSeq, m_psPermute.m_pPerm, m_lSeqLength);
//        m_psPermute.m_tPos++;
//        return true;
    }

///*
// * mscoremanager contains static short-cuts for dealing with mscore
// * plug-ins.
// */
//     final char* mscoremanager::TYPE = "scoring, algorithm";
//
///*
// * create_mscore creates the correct mscore for the given set of XML
// * parameters.
// */
//    mscore* mscoremanager::create_mscore(XmlParameter &_x)
//    {
//        String strValue;
//        String strKey = TYPE;
//        if (!_x.get(strKey,strValue))
//            strValue = "tandem";
//    #ifndef PLUGGABLE_SCORING
//        else
//        {
//            cerr << "Pluggable scoring is disabled.\n"
//                << "Rebuild X! Tandem with PLUGGABLE_SCORING defined, or\n"
//                << "remove 'scoring, algorithm' from your parameters.\n";
//            exit(EXIT_FAILURE);
//        }
//    #endif
//
//        return (mscore*) mpluginmanager::get().create_plugin(TYPE, strValue.data());
//    }
//
///*
// * register_factory registers a factory with the mpluginmanager for
// * creating mscore derived objects.
// */
//    void mscoremanager::register_factory( final char* _spec, mpluginfactory* _f)
//    {
//        mpluginmanager::get().register_factory(TYPE, _spec, _f);
//    }

}

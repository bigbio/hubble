package com.lordjoe.distributed.hydra.comet;

import com.lordjoe.distributed.hydra.test.TestUtilities;
import org.systemsbiology.xtandem.ionization.IonType;

import java.io.Serializable;
import java.util.Comparator;

/**
 * com.lordjoe.distributed.hydra.comet.BinnedChargeIonIndex
 * User: Steve
 * similat to p_uiBinnedIonMasses in comet
 * Date: 4/3/2015
 */
public class BinnedChargeIonIndex implements Comparable<BinnedChargeIonIndex>,Serializable {

    public static final Comparator<BinnedChargeIonIndex>   BY_INDEX = (o1, o2) -> {
        int ret = Integer.compare(o1.index, o2.index);
        if (ret != 0)
            return ret;
        ret = o1.type.compareTo(o2.type);
        if (ret != 0)
            return ret;
        ret = Integer.compare(o1.peptidePosition, o2.peptidePosition);
        if (ret != 0)
            return ret;
        return Integer.compare(o1.charge, o2.charge);
    };


    public static final Comparator<BinnedChargeIonIndex>  BY_BIN  = (o1, o2) -> {
        int ret = Integer.compare(o1.index, o2.index);
       if (ret != 0)
           return ret;
          return 0;
    };

    public final int index;
    public final int charge;
    public final IonType type;
    public final int peptidePosition;

    public BinnedChargeIonIndex(final int pIndex, final int pCharge, final IonType pType, final int pPeptidePosition) {
        index = pIndex;
        if(index == 12680)
            TestUtilities.breakHere();

        charge = pCharge;
        type = pType;
        if(pPeptidePosition < 0)
            throw new IllegalStateException("problem"); // ToDo change
        peptidePosition = pPeptidePosition;
    }

    @Override
    public String toString() {
        return "BinnedChargeIonIndex{" +
                "index=" + index +
                ", charge=" + charge +
                ", type=" + type +
                ", peptidePosition=" + peptidePosition +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final BinnedChargeIonIndex that = (BinnedChargeIonIndex) o;

        if(type != that.type) return false;
        if (index != that.index) return false;
        if (charge != that.charge) return false;
        if (peptidePosition != that.peptidePosition) return false;
        if (index == that.index)
            return true;
        return false; // look only index off

    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + charge;
        result = 31 * result + type.hashCode();
        result = 31 * result + peptidePosition;
        return result;
    }

    /**
     * sort on charge, type, position
     * @param o
     * @return
     */
    @Override
    public int compareTo(final BinnedChargeIonIndex o) {
        int ret = Integer.compare(charge, o.charge);
        if (ret != 0)
            return ret;
        ret = type.compareTo(o.type);
        if (ret != 0)
            return ret;
        ret = Integer.compare(peptidePosition, o.peptidePosition);
        if (ret != 0)
            return ret;
        return Integer.compare(index, o.index);
    }

}

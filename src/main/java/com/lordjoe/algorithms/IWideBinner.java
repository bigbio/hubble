package com.lordjoe.algorithms;

/**
 * com.lordjoe.algorithms.IWideBinner
 * a values may be placed in adjacent bins
 * User: Steve
 * Date: 6/28/13
 */
public interface IWideBinner extends IBinner {

    /**
     * give a list of all bins that the value may be assigned to
     * @param value  value to test
     * @return !null array of bins
     */
    int[] asBins(double value);


    /**
     * Describe the assigned bins
     * @param value
     * @return either a valid bin number or  null if  isOverflowBinned() is false and the
     *     data is outside the range handled
     */
    String[] asBinStrings(double value);

}

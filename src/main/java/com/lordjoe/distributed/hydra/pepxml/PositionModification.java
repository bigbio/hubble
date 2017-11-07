package com.lordjoe.distributed.hydra.pepxml;

/**
 * com.lordjoe.distributed.hydra.pepxml.PositionModification
 *
 * @author Steve Lewis
 * @date 5/18/2015
 */
public class PositionModification {
    public final int position;
    public final double massChange;

    public PositionModification(int position, double massChange) {
        this.position = position;
        this.massChange = massChange;
    }

    @Override
    public String toString() {
        return "PositionModification{" +
                "position=" + position +
                ", massChange=" + massChange +
                '}';
    }

    public String toModString() {
        String sb = "[" +
                String.format("%10.3f", massChange).trim() +
                "]";
        return sb;
    }
}

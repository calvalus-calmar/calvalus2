package com.bc.calvalus.binning;

/**
 * The bin manager class comprises a number of {@link Aggregator}s
 *
 * @author Norman Fomferra
 */
public class BinManager {

    private final Aggregator[] aggregators;
    private final int spatialFeatureCount;
    private final int temporalFeatureCount;
    private final int outputFeatureCount;
    private final int[] spatialFeatureOffsets;
    private final int[] temporalFeatureOffsets;
    private final int[] outputFeatureOffsets;
    private final String[] outputFeatureNames;
    private final double[] outputFeatureFillValues;

    public BinManager(Aggregator... aggregators) {
        this.aggregators = aggregators.clone();
        this.spatialFeatureOffsets = new int[aggregators.length];
        this.temporalFeatureOffsets = new int[aggregators.length];
        this.outputFeatureOffsets = new int[aggregators.length];
        int spatialFeatureCount = 0;
        int temporalFeatureCount = 0;
        int outputFeatureCount = 0;
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            spatialFeatureOffsets[i] = spatialFeatureCount;
            temporalFeatureOffsets[i] = temporalFeatureCount;
            outputFeatureOffsets[i] = outputFeatureCount;
            spatialFeatureCount += aggregator.getSpatialFeatureNames().length;
            temporalFeatureCount += aggregator.getTemporalFeatureNames().length;
            outputFeatureCount += aggregator.getOutputFeatureNames().length;
        }
        this.spatialFeatureCount = spatialFeatureCount;
        this.temporalFeatureCount = temporalFeatureCount;
        this.outputFeatureCount = outputFeatureCount;
        this.outputFeatureNames = new String[outputFeatureCount];
        this.outputFeatureFillValues = new double[outputFeatureCount];
        for (int i = 0, k = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            for (int j = 0; j < aggregator.getOutputFeatureNames().length; j++) {
                outputFeatureNames[k] = aggregator.getOutputFeatureNames()[j];
                outputFeatureFillValues[k] = aggregator.getOutputFillValue();
                k++;
            }
        }
    }

    public final int getSpatialFeatureCount() {
        return spatialFeatureCount;
    }

    public final int getTemporalFeatureCount() {
        return temporalFeatureCount;
    }

    public final String[] getOutputFeatureNames() {
        return outputFeatureNames;
    }

    public final double getOutputFeatureFillValue(int i) {
        return outputFeatureFillValues[i];
    }

    public final int getAggregatorCount() {
        return aggregators.length;
    }

    public final Aggregator getAggregator(int aggIndex) {
        return aggregators[aggIndex];
    }

    public final Vector getSpatialVector(SpatialBin bin, int aggIndex) {
        final VectorImpl vector = new VectorImpl(bin.featureValues);
        final Aggregator aggregator = aggregators[aggIndex];
        vector.setOffsetAndSize(spatialFeatureOffsets[aggIndex], aggregator.getSpatialFeatureNames().length);
        return vector;
    }

    public final Vector getTemporalVector(TemporalBin bin, int aggIndex) {
        final VectorImpl vector = new VectorImpl(bin.featureValues);
        final Aggregator aggregator = aggregators[aggIndex];
        vector.setOffsetAndSize(temporalFeatureOffsets[aggIndex], aggregator.getTemporalFeatureNames().length);
        return vector;
    }

    public WritableVector createOutputVector() {
        return new VectorImpl(new float[outputFeatureCount]);
    }

    public SpatialBin createSpatialBin(long binIndex) {
        final SpatialBin spatialBin = new SpatialBin(binIndex, spatialFeatureCount);
        initSpatialBin(spatialBin);
        return spatialBin;
    }

    public void aggregateSpatialBin(Observation observation, SpatialBin spatialBin) {
        final VectorImpl spatialVector = new VectorImpl(spatialBin.featureValues);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            spatialVector.setOffsetAndSize(spatialFeatureOffsets[i], aggregator.getSpatialFeatureNames().length);
            aggregator.aggregateSpatial(spatialBin, observation, spatialVector);
        }
        spatialBin.numObs++;
    }

    public void completeSpatialBin(SpatialBin spatialBin) {
        final VectorImpl spatialVector = new VectorImpl(spatialBin.featureValues);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            spatialVector.setOffsetAndSize(spatialFeatureOffsets[i], aggregator.getSpatialFeatureNames().length);
            aggregator.completeSpatial(spatialBin, spatialBin.numObs, spatialVector);
        }
    }

     public TemporalBin createTemporalBin(long binIndex) {
         final TemporalBin temporalBin = new TemporalBin(binIndex, temporalFeatureCount);
         initTemporalBin(temporalBin);
         return temporalBin;
     }

    public void aggregateTemporalBin(SpatialBin inputBin, TemporalBin outputBin) {
        aggregateBin(inputBin, outputBin);
        outputBin.numPasses++;
    }

    public void aggregateTemporalBin(TemporalBin inputBin, TemporalBin outputBin) {
        aggregateBin(inputBin, outputBin);
        outputBin.numPasses += inputBin.numPasses;
    }

    private void aggregateBin(Bin inputBin, Bin outputBin) {
        final VectorImpl spatialVector = new VectorImpl(inputBin.featureValues);
        final VectorImpl temporalVector = new VectorImpl(outputBin.featureValues);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            spatialVector.setOffsetAndSize(spatialFeatureOffsets[i], aggregator.getSpatialFeatureNames().length);
            temporalVector.setOffsetAndSize(temporalFeatureOffsets[i], aggregator.getTemporalFeatureNames().length);
            aggregator.aggregateTemporal(outputBin, spatialVector, inputBin.numObs, temporalVector);
        }
        outputBin.numObs += inputBin.numObs;
    }

    public void completeTemporalBin(TemporalBin temporalBin) {
        final VectorImpl temporalVector = new VectorImpl(temporalBin.featureValues);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            temporalVector.setOffsetAndSize(spatialFeatureOffsets[i], aggregator.getSpatialFeatureNames().length);
            aggregator.completeTemporal(temporalBin, temporalBin.numObs, temporalVector);
        }
    }

    public void computeOutput(TemporalBin temporalBin, WritableVector outputVector) {
        final VectorImpl temporalVector = new VectorImpl(temporalBin.featureValues);
        final VectorImpl outputVectorImpl = (VectorImpl) outputVector;
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            temporalVector.setOffsetAndSize(temporalFeatureOffsets[i], aggregator.getTemporalFeatureNames().length);
            outputVectorImpl.setOffsetAndSize(outputFeatureOffsets[i], aggregator.getOutputFeatureNames().length);
            aggregator.computeOutput(temporalVector, outputVector);
        }
        outputVectorImpl.setOffsetAndSize(0, outputFeatureCount);
    }

    protected void initSpatialBin(SpatialBin bin) {
        final VectorImpl vector = new VectorImpl(bin.featureValues);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            vector.setOffsetAndSize(spatialFeatureOffsets[i], aggregator.getSpatialFeatureNames().length);
            aggregator.initSpatial(bin, vector);
        }
    }

    protected void initTemporalBin(TemporalBin bin) {
        final VectorImpl vector = new VectorImpl(bin.featureValues);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            vector.setOffsetAndSize(temporalFeatureOffsets[i], aggregator.getTemporalFeatureNames().length);
            aggregator.initTemporal(bin, vector);
        }
    }
}

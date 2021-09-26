package edu.coursera.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    protected static double f(double x) {
        return Math.pow(Math.pow(Math.sin(x), 2) + Math.pow(Math.cos(x), 2) + 1, 4);
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += f(input[i]);
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveTask<Double> {
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Threshold value for creating subtasks.
         */
        private final int threshold;
        /**
         * Function for computation.
         */
        private final DoubleUnaryOperator function;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values.
         * @param setThreshold Threshold for subtasks creation.
         * @param setFunction Function for computation.
         */
        ReciprocalArraySumTask(
            final int setStartIndexInclusive,
            final int setEndIndexExclusive,
            final double[] setInput,
            final int setThreshold,
            final DoubleUnaryOperator setFunction
        ) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.threshold = setThreshold;
            this.function = setFunction;
        }

        @Override
        protected Double compute() {
            final int currentLength = this.endIndexExclusive - this.startIndexInclusive;

            if (currentLength <= this.threshold) {
                Double result = 0.0;

                for (int i = this.startIndexInclusive; i < this.endIndexExclusive; i++) {
                    result += this.function.applyAsDouble(this.input[i]);
                }

                return result;
            } else {
                final ReciprocalArraySumTask masterTask = new ReciprocalArraySumTask(
                    this.startIndexInclusive,
                    this.endIndexExclusive - currentLength / 2,
                    this.input,
                    this.threshold,
                    this.function
                );
                final ReciprocalArraySumTask slaveTask = new ReciprocalArraySumTask(
                    this.endIndexExclusive - currentLength / 2,
                    this.endIndexExclusive,
                    this.input,
                    this.threshold,
                    this.function
                );
                slaveTask.fork();

                return masterTask.compute() + slaveTask.join();
            }
        }
    }

    /**
     * Computes the same reciprocal sum as seqArraySum, but use two tasks
     * running in parallel under the Java ForkJoin framework.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        return parManyTaskArraySum(input, 2);
    }

    /**
     * Uses a set number of tasks to compute the reciprocal array sum.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(
        final double[] input,
        final int numTasks
    ) {
        return ForkJoinPool.commonPool().invoke(new ReciprocalArraySumTask(
            0,
            input.length,
            input,
            getChunkSize(numTasks, input.length),
            ReciprocalArraySum::f
        ));
    }
}

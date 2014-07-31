package cn.edu.sysu.distributedLSH.common;

import static java.lang.Math.*;

import java.util.Random;


public class LSHTool {
    private static final double DOUBLE_ZERO = 1e-6;

    private static Random random = new Random();


    /**
     * For safe.
     * */
    private LSHTool() {}

    /**
     * Given the mean and the standard deviation, genearlGaussian generates
     * a normally distributed random number.
     * @param mean the mean of the Gaussian distribution
     * @param deviation the deviation of the Gaussian distribution
     * */
    public static double generalGaussian(final double mean,
            final double deviation) {
        return random.nextGaussian() * deviation + mean;
    }

    /**
     * Generates a uniformly distributed random number between min and max.
     * @param min the lower bound
     * @param max the upper bound
     * */
    public static double generalUniform(final double min, final double max) {
        return random.nextDouble() * (max - min) + min;
    }

    /**
     * Generates a random number that follows Zipf distribution and
     * lies between min and max.
     * @param min the lower bound
     * @param max the upper bound
     * @param p
     * */
    public static double zipf(final double min, final double max,
            final double p) {
        double r, HsubV, sum;
        int V = 100;

        // calculate the V-th harmonic number HsubV. WARNING: V > 1
        HsubV = 0;
        for (int i = 1; i <= V; i++) {
            HsubV += 1.0 / pow(i, p);
        }
        r = generalUniform(0.0, 1.0) * HsubV;
        sum = 1.0;

        double standard = generalUniform(0.0, 1.0);
        double result;
        while (sum < r) {
            standard += generalUniform(1.0, 2.0);
            sum += 1.0 / pow(standard, p);
        }

        // standard follows Zipf distribution and lies between 1 and V
        // result lies between 0.0 and 1.0 and then between min and max
        result = (standard - 1.0) / ((double)V - 1.0);
        result = (max - min) * result + min;
        return result;
    }

    /**
     * Generate a number where each of its digits is distributed
     * uniformly from [0, 9].
     * @param numOfDigits the number of digits of a generated number
     * */
    public static double digitUniform(final int numOfDigits) {
        double base = 1.0;
        double sum = 0.0;
        int digit;

        for (int i = 0; i < numOfDigits; i++) {
            // generate digit from [0, 9]
            digit = (int)generalUniform(0, 10);
            if (10 == digit) {
                digit = 9;
            }
            sum += base * digit;
            base *= 10;
        }
        return sum;
    }

    /**
     * Generate a number where each of its 10 digits is distributed
     * uniformly from [0, 9]. Meanwhile, it is in the range of
     * [min, max].
     * @param min the lower bound
     * @param max the upper bound
     * */
    public static double boundedDigitUniform(final double min, final double max) {
        final double base = 9999999999.0;
        double result = digitUniform(10);

        result = result / base * (max - min) + min;
        return result;
    }

    /**
     * Calculate the probability density function of the Gaussian
     * distribution.
     * @param x
     * @param mean the mean of the Gaussian distribution
     * @param deviation the deviation of the Gaussian distribution
     * */
    public static double normalPdf(final double x, final double mean,
            final double deviation) {
        double result;

        result = exp(-(x-mean) * (x-mean) / (2*deviation*deviation));
        result /= deviation * sqrt(2 * PI);
        return result;
    }

    /**
     * Calculate the cumulative distribution function of the standard
     * Gaussian distribution.
     * @param x
     * @param step It controls the precision of the result. The recommended
     *        value is 0.001.
     * */
    public static double standardNormalCdf(final double x, final double step) {
        double result = 0;

        for (double i = -10; i < x; i += step) {
            result += step * normalPdf(i, 0.0, 1.0);
        }
        return result;
    }

    /**
     * Compare two int variables.
     * @param a int
     * @param b int
     * @return
     * 0:  a = b
     * 1:  a > b
     * -1: a < b
     * */
    public static int compareInts(final int a, final int b) {
        return a < b ? -1 : (a > b ? 1 : 0);
    }

    /**
     * Compare two double numbers. For the equality, the error is in
     * [-10^-6, 10^-6].
     * @param a the first double number
     * @param b the second double number
     * @return
     * 1:  a > b
     * -1: a < b
     * 0:  a = b
     * */
    public static int compareDouble(final double a, final double b) {
        int result;

        if (a - b > DOUBLE_ZERO) {
            result = 1;
        } else if (b - a > DOUBLE_ZERO) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }

    /**
     * Calculate the L2 distance of two vectors whose dimensionality are dim.
     * @param a the first vector
     * @param b the second vector
     * @param dim the dimensionality of the vector
     * */
    public static double calcL2Distance(int[] a, int[] b, final int dim) {
        double difference, distance = 0;

        for (int i = 0; i < dim; i++) {
            difference = a[i] - b[i];
            distance += difference * difference;
        }
        return sqrt(distance);
    }

    /**
     * Print a string then exit.
     * @param str
     * */
    public static void printAndExit(final String str) {
        System.err.println(str);
        System.exit(1);
    }
    
    /**
     * Convert seconds to HH:MM:SS format then return a string.
     * @param time time in seconds
     * */
    public static String convertTime(final int time) {
        int hour = time / 3600;
        int minute = (time % 3600) / 60;
        int second = time % 60;

        return timeDigit(hour) + ":" + timeDigit(minute) + ":" + timeDigit(second);
    }
    
    /**
     * Convert a number to a string following time format.
     * @param num
     * */
    private static String timeDigit(final int num) {
        if (0 == num) {
            return "00";
        } else if (num < 10) {
            return "0" + num;
        }
        return String.valueOf(num);
    }
}

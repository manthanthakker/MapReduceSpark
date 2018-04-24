package loader;

/**
 * Implements methods to Calculate Fibonacci series
 */
public class FibonacciCalculator {

    /**
     *
     * @param n: Given an input
     * @return the fibonacci of the number n
     */
    public static long fib(int n) {
        if (n <= 1) return n;
        else return fib(n-1) + fib(n-2);
    }
}

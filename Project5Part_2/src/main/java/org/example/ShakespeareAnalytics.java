/**
 Name: Siddhesh Badhan
 Andrew ID: sbadhan
 This class provides methods to perform analytics on a given text file containing Shakespeare's works.
 It uses Apache Spark to process the data in a distributed manner.
 */

package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Scanner;

public class ShakespeareAnalytics {

    //The Spark configuration used to create a JavaSparkContext object.
    static SparkConf sConf = new SparkConf().setMaster("local").setAppName("ShakespeareAnalytics");
    //The JavaSparkContext object used to read and process data from the input file.
    static JavaSparkContext sContext = new JavaSparkContext(sConf);


    /**

     This method counts the number of lines in the input file and prints the result.
     @param fileName The name of the input file to be processed.
     */
    private static void numberOfLines(String fileName) {
        // Read the file contents as an RDD of strings
        JavaRDD<String> inputFile = sContext.textFile(fileName);
        // Split each string in the RDD into lines and flatten the resulting list of lists into a single list
        JavaRDD<String> lines = inputFile.flatMap(content -> Arrays.asList(content.split("\n")));
        // Count the number of elements in the RDD and print the count to the console
        long lineCount = lines.count();
        System.out.println("Number of lines: " + lineCount);
    }

    /**
     This method counts the number of words in the input file and prints the result.
     @param fileName The name of the input file to be processed.
     */
    private static void numberOfWords(String fileName) {
        // Read the file using Spark and split it into words
        JavaRDD<String> words = sContext.textFile(fileName)
                .flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+")))
                .filter(word -> !word.isEmpty());
        // Count the number of words and print the result
        System.out.println("Number of words: " + words.count());
    }

    /**
     This method counts the number of distinct words in the input file and prints the result.
     @param fileName The name of the input file to be processed.
     */
    private static void numberOfDistinctWords(String fileName) {
        // Split each line into words and filter out any empty words or non-alphabetic characters
        JavaRDD<String> words = sContext.textFile(fileName)
                .flatMap(content -> Arrays.asList(content.split("[^a-zA-Z]+")))
                .filter(word -> !word.isEmpty())
                .distinct();

        // Count the number of distinct words and print the result
        long distinctWordCount = words.distinct().count();
        System.out.println("Number of distinct words: " + distinctWordCount);
    }

    /**
     This method counts the number of symbols in the input file and prints the result.
     @param fileName The name of the input file to be processed.
     */
    private static void numberOfSymbols(String fileName) {
        // Split each string into individual characters using the flatMap transformation
        JavaRDD<String> symbols = sContext.textFile(fileName)
                .flatMap(content -> Arrays.asList(content.split("")))
                .filter(symbol -> !symbol.trim().isEmpty());
        // Filter out empty symbols using the trim method, and count the remaining symbols
        System.out.println("Number of symbols: " + symbols.filter(symbol -> !symbol.trim().isEmpty()).count());
    }

    /**
     This method counts the number of distinct symbols in the input file and prints the result.
     @param fileName The name of the input file to be processed.
     */
    private static void numberOfDistinctSymbols(String fileName) {
        // Split each line into individual symbols
        JavaRDD<String> symbols = sContext.textFile(fileName)
                .flatMap(content -> Arrays.asList(content.split("")));
        // Count the number of distinct symbols
        long distinctCount = symbols.distinct().count();
        System.out.println("Number of distinct symbols: " + distinctCount);
    }

    /**
     This method counts the number of distinct letters in the input file and prints the result.
     @param fileName The name of the input file to be processed.
     */
    private static void numberOfDistinctLetters(String fileName) {
        // Read the file and split its content into individual characters &
        // Filter out any non-letter characters and empty strings
        JavaRDD<String> letters = sContext.textFile(fileName)
                .flatMap(content -> Arrays.asList(content.split("")))
                .filter(str -> !str.isEmpty() && str.matches("[a-zA-Z]"))
                .distinct();
        // Count the number of distinct letters and print the result
        long distinctCount = letters.distinct().count();
        System.out.println("Number of distinct letters: " + distinctCount);
    }

    /**
     This method searches for a given word in the input file and prints all lines that contain the word.
     @param fileName The name of the input file to be searched.
     */
    private static void search(String fileName) {
        // It prompts the user to input the keyword they want to search.
        System.out.println("Insert the word you want to search");
        Scanner scanner = new Scanner(System.in);
        String key = scanner.nextLine();

        // The JavaRDD<String> object "words" is created by filtering the text file based on whether it contains the keyword.
        JavaRDD<String> words = sContext.textFile(fileName)
                .filter(line -> line.contains(key));

        // Finally, the foreach() method is used to print out all the lines containing the keyword
        // to the console, and the scanner object is closed.
        words.foreach(word -> System.out.println(word));
        scanner.close();
    }

    /**
     The main method of the program.
     It takes the name of the input file as a command line argument and calls all the methods to perform analytics on the file.
     @param args Command line arguments, where the first argument is the name of the input file to be processed.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Input not available");
            System.exit(0);
        }
        String fileName = args[0];
        numberOfLines(fileName);
        numberOfWords(fileName);
        numberOfDistinctWords(fileName);
        numberOfSymbols(fileName);
        numberOfDistinctSymbols(fileName);
        numberOfDistinctLetters(fileName);
        search(fileName);
    }
}
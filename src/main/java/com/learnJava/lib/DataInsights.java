package com.learnJava.lib;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DataInsights {
    public static void main(String[] args) throws FileNotFoundException {
        Path path = Paths.get ("E:\\JavaSpark\\Retail_Analytics_Platform\\src\\main\\resources\\sample.txt");

        File file = new File (path.toUri());
        if (file.exists()) {
            System.out.println (file.getName() + " File exists");
            try (BufferedInputStream bf = new BufferedInputStream(new FileInputStream(file))) {
                int c;
                StringBuilder line = new StringBuilder();
                while ((c = bf.read()) != -1) {
                    line.append((char) c);
                }
                System.out.println (line);
            } catch (Exception e) {
                System.out.println ("Error occurred : " + e.getMessage());
            }
        }
        System.out.println ("\n");

        if (file.exists()) {
            try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
                String line = "";
                while ((line = bf.readLine()) != null) {
                    System.out.println (line);
                }

            } catch (IOException ioe) {
                System.out.println ("Exception occurred : " + ioe.getMessage());
            }
        }
    }
}

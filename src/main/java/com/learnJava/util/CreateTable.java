package com.learnJava.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class CreateTable {
    public static void main(String[] args) {
        File file = new File(CreateTable.class.getResource("/queryFiles/create_table.sql").getFile());
        try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
            StringBuilder query = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null) {
                query.append(line);
                query.append("\n");
            }
            System.out.println ("Query : \n" + query);
        } catch (IOException io) {
            System.out.println ("Error : " + io.getMessage());
        }
    }
}

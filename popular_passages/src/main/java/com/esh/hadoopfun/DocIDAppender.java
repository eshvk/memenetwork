package com.esh.hadoopfun;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DocIDAppender {
    private String inputFile;
    private String outputFile;
    public DocIDAppender(String inputFile, String outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }
    public void append(){
        BufferedReader bInput;
        BufferedWriter bOutput;
        try {
            bInput = new BufferedReader(new FileReader(inputFile));
            bOutput = new BufferedWriter(new FileWriter(outputFile));
            String currLine;
            int count = 0;
            while ((currLine = bInput.readLine())!= null) {
                ++count;
                bOutput.write(count + "\t" + currLine + "\n");
            }
            bInput.close();
            bOutput.close();
        } catch (IOException e) {
            System.err.println("Error has occurred " + e);
        }
    }
    public static void main (String [] args) {
        DocIDAppender dIA = new DocIDAppender(args[0], args[1]);
        dIA.append();
    }
}

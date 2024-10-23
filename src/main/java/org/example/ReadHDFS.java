package org.example;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        // Check if the filename is provided
        if (args.length != 1) {
            System.out.println("Usage: hadoop jar ReadHDFS.jar <filename>");
            return;
        }

        // Create a configuration object and get the filesystem instance
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Use the filename provided as the first argument
        Path nomcomplet = new Path(args[0]); // Get the filename from args

        // Open the file and read its content
        try (FSDataInputStream inStream = fs.open(nomcomplet);
             BufferedReader br = new BufferedReader(new InputStreamReader(inStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line); // Print each line
            }
        } catch (FileNotFoundException e) {
            System.out.println("Le fichier n'existe pas : " + args[0]);
        } catch (IOException e) {
            System.out.println("Erreur lors de la lecture du fichier : " + e.getMessage());
        } finally {
            fs.close(); // Ensure the filesystem is closed
        }
    }
}

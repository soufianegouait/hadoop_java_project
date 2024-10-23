package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {
        // Check if enough arguments are provided
        if (args.length < 2) {
            System.out.println("Usage: hadoop jar WriteHDFS.jar <output_path> <additional_string>");
            return;
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path(args[0]);

        // Check if the file already exists
        if (!fs.exists(nomcomplet)) {
            FSDataOutputStream outStream = fs.create(nomcomplet);
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(args[1]); // Use the second argument
            outStream.close();
        } else {
            System.out.println("Le fichier existe déjà : " + args[0]);
        }

        fs.close();
    }
}

package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

public class HDFSInfo {
    public static void main(String[] args) throws IOException {
        // Check if the filename is provided
        if (args.length != 1) {
            System.out.println("Usage: hadoop jar HDFSInfo.jar <filename>");
            return;
        }

        // Obtain an object that represents HDFS
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Use the filename provided as the first argument
        Path nomcomplet = new Path(args[0]); // Get the filename from args
        if (!fs.exists(nomcomplet)) {
            System.out.println("Le fichier n'existe pas");
        } else {
            // Display file size
            FileStatus infos = fs.getFileStatus(nomcomplet);
            System.out.println(Long.toString(infos.getLen()) + " octets");
            // List blocks of the file
            BlockLocation[] blocks = fs.getFileBlockLocations(infos, 0, infos.getLen());
            System.out.println("Le nombre de blocks est: " + blocks.length + " blocs :");
            for (BlockLocation blocloc : blocks) {
                System.out.println("\t" + blocloc.toString() + " length: " + blocloc.getLength());
            }
        }
        // Close HDFS
        fs.close();
    }
}

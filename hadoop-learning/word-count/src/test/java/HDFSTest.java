import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author : wzdnzd
 * @Time : 2019-06-07
 * @Project : bigdata
 */


public class HDFSTest {
    Configuration conf = null;
    FileSystem fs = null;

    @Before
    public void conn() throws IOException {
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void getValue() {
        String value = conf.get("fs.default.name");
        System.out.println(value);
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/learn/data/test");
        boolean flag = false;

        if (!fs.exists(path))
            flag = fs.mkdirs(path);

        System.out.println("success: " + flag);
    }

    @Test
    public void delete() throws IOException {
        Path path = new Path("/learn/data/test");
        if (fs.exists(path))
            fs.delete(path, true);

        System.out.println("success: " + !fs.exists(path));
    }

    @Test
    public void listFiles() throws IOException {
        Path path = new Path("/learn/data/wordcount");

        if (fs.exists(path)) {
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
            while (files != null && files.hasNext()) {
                LocatedFileStatus file = files.next();
                if (file.isFile())
                    System.out.println("file: " + file.getPath() + "\t" + "owner: " + file.getOwner());
            }
        }
    }
}

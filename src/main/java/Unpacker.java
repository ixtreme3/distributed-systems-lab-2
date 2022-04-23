import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;

public class Unpacker {
    public static void bz2ToOsm() throws IOException {
        if (!(new File("RU-NVS.osm").isFile())) {
            try (FileInputStream in = new FileInputStream("RU-NVS.osm.bz2");
                 FileOutputStream out = new FileOutputStream("RU-NVS.osm")) {
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(new BufferedInputStream(in));
                final byte[] buffer = new byte[4096*32];
                int n = 0;
                while (-1 != (n = bzIn.read(buffer))) {
                    out.write(buffer, 0, n);
                }
            }
            ;
        }
    }
}

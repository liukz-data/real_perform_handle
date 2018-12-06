package cn.hbwy.dealavro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;

import java.io.Serializable;

public class DataFileWriterImpl<D> extends DataFileWriter<D>  implements Serializable {
    /**
     * Construct a writer, not yet open.
     *
     * @param dout
     */
    public DataFileWriterImpl(DatumWriter<D>  dout) {
        super(dout);
    }
}

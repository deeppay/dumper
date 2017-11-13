package carldata.dumper;

import com.datastax.driver.mapping.annotations.Table;

import java.util.List;

@Table(name = "real_time_jobs")
public class RealTimeJob {

    public String calculation;
    public String script;
    public List<String> input_channels;
    public String output_channel;
}

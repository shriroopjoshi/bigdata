package classes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class TextArrayWritable extends ArrayWritable {

	public TextArrayWritable(Text[] values) {
		super(Text.class, values);

	}

	@Override
	public Text[] get() {
		return (Text[]) super.get();
	}

	@Override
	public String toString() {
		Text[] texts = this.get();
		StringBuilder sb = new StringBuilder("");
		for (int i = 0; i < texts.length; ++i) {
			if (i == texts.length - 1)
				sb.append(texts[i].toString());
			else
				sb.append(texts[i].toString() + ",");
		}
		return sb.toString();
	}

}

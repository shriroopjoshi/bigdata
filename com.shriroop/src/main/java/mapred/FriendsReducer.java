package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import classes.FriendsPair;
import classes.TextArrayWritable;

public class FriendsReducer extends Reducer<FriendsPair, Text, FriendsPair, TextArrayWritable> {

	@Override
	protected void reduce(FriendsPair pair, Iterable<Text> values,
			Reducer<FriendsPair, Text, FriendsPair, TextArrayWritable>.Context output)
			throws IOException, InterruptedException {
		List<Text> list = new ArrayList<Text>();
		Iterator<Text> itr = values.iterator();
		while(itr.hasNext()) {
			Text friend = itr.next();
			list.add(friend);
		}
		Text[] arr = new Text[list.size()];
		arr = list.toArray(arr);
		output.write(pair, new TextArrayWritable(arr));
	}

}

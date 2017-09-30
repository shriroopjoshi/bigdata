package mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Reducer;

import classes.FriendsPair;

public class FriendsReducer extends Reducer<FriendsPair, ArrayWritable, FriendsPair, ArrayWritable> {

	@Override
	protected void reduce(FriendsPair pair, Iterable<ArrayWritable> friendList,
			Reducer<FriendsPair, ArrayWritable, FriendsPair, ArrayWritable>.Context output)
			throws IOException, InterruptedException {
		ArrayList<String> arr = new ArrayList<>();
		for (Iterator iterator = friendList.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			arr.add(string);
		}
		String[] str = new String[arr.size()];
		output.write(pair, new ArrayWritable(arr.toArray(str)));
	}

}

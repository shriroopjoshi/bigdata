package mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import classes.FriendsPair;

public class FriendsMapper extends Mapper<Object, Text, FriendsPair, String> {

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, FriendsPair, String>.Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		String person = fields[0].trim();
		String[] friends = fields[1].split(",");
		String[] set = new String[friends.length];
		for (int i = 0; i < friends.length; i++) {
			set[i] = friends[i].trim();
		}
		for (int i = 0; i < friends.length; ++i) {
			FriendsPair fp = new FriendsPair();
			fp.setFirstPerson(person);
			fp.setSecondPerson(friends[i].trim());
			for (int j = 0; j < set.length; ++j) {
				if(!set[j].equals(friends[i])) {
					context.write(fp, set[j]);
				}
			}
		}
	}
	
}

package classes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FriendsPair implements Writable, WritableComparable<FriendsPair> {

	String firstPerson;
	String secondPerson;
	
	@Override
	public int compareTo(FriendsPair friend) {
		if(firstPerson.compareTo(friend.firstPerson) == 0
				&& secondPerson.compareTo(friend.secondPerson) == 0)
			return 0;
		else if(firstPerson.compareTo(friend.secondPerson) == 0
				&& secondPerson.compareTo(friend.firstPerson) == 0)
			return 0;
		else {
			if(firstPerson.compareTo(friend.firstPerson) < 0
					|| (firstPerson.compareTo(friend.firstPerson) == 0
					&& secondPerson.compareTo(friend.secondPerson) < 0))
				return -1;
			else
				return 1;
		}
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		firstPerson = input.readUTF();
		secondPerson = input.readUTF();
	}
	
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(firstPerson);
		output.writeUTF(secondPerson);
	}
	
	public void setFirstPerson(String person) {
		this.firstPerson = person;
	}
	
	public void setSecondPerson(String person) {
		this.secondPerson = person;
	}
	
	public String getFirstPerson() {
		return this.firstPerson;
	}
	
	public String getSecondPerson() {
		return this.secondPerson;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstPerson == null) ? 0 : firstPerson.hashCode());
		result = prime * result + ((secondPerson == null) ? 0 : secondPerson.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FriendsPair other = (FriendsPair) obj;
		if(firstPerson.equals(other.firstPerson)) {
			if(secondPerson.equals(other.secondPerson))
				return true;
			else
				return false;
		} else if(firstPerson.equals(other.secondPerson)) {
			if(secondPerson.equals(other.firstPerson))
				return true;
			else
				return false;
		} else
			return false;
	}

	@Override
	public String toString() {
		return "[" + firstPerson + ", " + secondPerson + "]";
	}

	
}

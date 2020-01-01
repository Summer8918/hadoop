import java.io.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;
    
    public TextPair() {
        set(new Text(), new Text());
    }
    
    public TextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }
    
    public TextPair(Text first, Text second) {
        set(first, second);
    }
    
    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }
    public Text getFirst() { 
        return first; 
    }
    public Text getSecond() { 
        return second; 
    }
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
    @Override
    public String toString() { 
        return first + "\t" + second; 
    }
    @Override
    public int compareTo(TextPair tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj){
        if(this==obj){
            return true;
        }
        if(obj==null){
            return false;
        }
        if(getClass()!=obj.getClass()){
            return false;
        }
        TextPair other = (TextPair)obj;
        if(first==null){
            if(other.first!=null){
                return false;
            }
        }
        else if(!first.equals(other.first)){
            return false;
        }
        if(second==null){
            if(other.second!=null){
                return false;
            }
        }
        else if(!second.equals(other.second)){
            return false;
        }
        return true;
    }
}

// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.util.ArrayList;
import java.util.List;

import julien.galago.core.types.WordDateCount;
import julien.galago.tupleflow.Reducer;
import julien.galago.tupleflow.Utility;


public class WordDateReducer implements Reducer<WordDateCount> {
    public ArrayList<WordDateCount> reduce(List<WordDateCount> input) {
	ArrayList<WordDateCount> output = new ArrayList<WordDateCount>();
	byte[] oldWord = null;
	int oldDate = 0;
	for (WordDateCount wdCount : input) {
	    if (oldWord == null || 
		Utility.compare(oldWord, wdCount.word) != 0 ||
		oldDate != wdCount.date) {
		output.add(wdCount);
		oldWord = wdCount.word;
		oldDate = wdCount.date;
	    } else {
		output.get(output.size()-1).count += wdCount.count;
	    }
	}
	return output;
    }
}
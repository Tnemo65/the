package kdrange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class KDTree {
	
	private KDTreeHelper<Integer> kd;
	
    public KDTree(int[][] arr) throws KeySizeException, KeyDuplicateException {
    	kd = new KDTreeHelper<Integer>(arr[0].length);
    	List<Integer> indices = new ArrayList<>();
    	for (int i = 0; i < arr.length; i++) {
    	    indices.add(i);
    	}
    	Collections.shuffle(indices);
    	for (int i : indices) kd.insert(arr[i], i);
    }
    
    
    public Collection<Integer> query(int[] from, int[] to) throws KeySizeException {
    	int[] toEx = new int[to.length];
    	for (int i = 0; i < to.length; i++) toEx[i] = to[i] - 1;
    	return kd.range(from, toEx);
    }
    
    public int queryCount(int[] from, int[] to) throws KeySizeException {
    	int[] toEx = new int[to.length];
    	for (int i = 0; i < to.length; i++) toEx[i] = to[i] - 1;
        return kd.rangeCount(from, toEx);
    }
}

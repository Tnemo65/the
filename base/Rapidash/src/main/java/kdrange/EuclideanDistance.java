// Hamming distance metric class

package kdrange;

class EuclideanDistance extends DistanceMetric {
    
    protected int distance(int [] a, int [] b)  {
	
	return (int) Math.sqrt(sqrdist(a, b));
	
    }
    
    protected static int sqrdist(int [] a, int [] b) {

	int dist = 0;

	for (int i=0; i<a.length; ++i) {
	    int diff = (a[i] - b[i]);
	    dist += diff*diff;
	}

	return dist;
    }     
}

package paper.analyze.sigmod;

public class MsgRateThreshold {
	/** Notice, using the expection to estimate the number of fragments is not 
	 *  accuracy!! give it up */
	
	static double alpha = 1.8;
	
	static int V = 61578403;
	static int E = 1489840304;
	static int T = 30;
	static int P = 66;
	static int N = T*P;
	
	public static void main(String[] args) {
		//System.out.println(Math.pow(V, -alpha));
		//System.out.println(Math.pow((1-1/(double)N), 1));
		System.out.println(Math.pow((1-(1/(double)N)), 24));
		
		double h_v = 0.0;
		double sum = 0.0;
		/*for (int d = 1; d < (V-1); d++) {
			h_v += Math.pow(d, -alpha);
			sum += (Math.pow((1-1/(double)N), d) * Math.pow(d, -alpha));
			//System.out.println(h_v + ", " + sum);
			if (d%1000000 == 0) {
				System.out.println("progress=" + (d/(float)V));
			}
		}*/
		//System.out.println("h_v=" + h_v + ", sum=" + sum);
		
		//double numOfF = N*V*(1-(1/h_v)*sum);
		double numOfF = N*V*(1-Math.pow((1-(1/(double)N)), 24));
		System.out.println("#F=" + numOfF);
		
		
		double buffer = (V*(T+2))/(double)P;
		System.out.println("#B=" + buffer);
		
		
		double threshold = (numOfF+buffer)/E;
		System.out.println("threshold=" + threshold);
	}
}

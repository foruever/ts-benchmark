package cn.edu.ruc;
/**
 * 
 * @author sxg
 */
public abstract class DB {
	public abstract void test();
	public static void main(String[] args) {
		long start=200873363343932L;
		long end=start+500000000*98;
		System.out.println(start);
		System.out.println(end);//200871323736380
	}
}


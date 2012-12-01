package edu.mayo.bior.indexer.cmd;

public class Junk {


	public enum JunkEnum { yo, ho;
		@Override
		public String toString() {
			return this.toString();
		}
	}
	
	public enum Strand {
		FORWARD {
		    public String toString() {
		        return "+";
		    }
		},
		 
		REVERSE {
		    public String toString() {
		        return "-";
		    }
		},
		
		UNKNOWN {
			public String toString() {
				return "?";
			}
		}
	};
		

	public static void main(String[] args) {
		JunkEnum j = JunkEnum.yo;
		System.out.println("enum: " + j);
		System.out.println(j.toString());
		System.out.println("ho enum: " + JunkEnum.ho);
		System.out.println("java version: " + System.getProperty("java.version"));
		System.out.println("Strand FORWARD: " + Strand.FORWARD);
		//takeIt(Strand.FORWARD);
	}
	
	public static void takeIt(String s) {
		System.out.println("s: " + s);
	}
}

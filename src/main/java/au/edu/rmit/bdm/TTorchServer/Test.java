package au.edu.rmit.bdm.TTorchServer;

public class Test {
    public static void main(String[] args){
        System.out.println(Test.class.getClassLoader().getResource("").getPath());
    }
}

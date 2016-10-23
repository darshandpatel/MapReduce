package code;

/**
 * Created by Darshan on 10/23/16.
 */
public class Test {

    public static void main(String args[]){
        double value1 = 1d;
        long longValue1 = Double.doubleToRawLongBits(value1);

        double value2 = 2d;
        long longValue2 = Double.doubleToRawLongBits(value2);

        System.out.println(longValue1 + "  :  "+longValue2);
        System.out.println(longValue1+longValue2);
        System.out.println( Double.doubleToLongBits(2));

        System.out.println(Double.longBitsToDouble(longValue1 + longValue2));
    }
}

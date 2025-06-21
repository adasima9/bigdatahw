//package tool;
//
//import org.roaringbitmap.RoaringBitmap;
//import org.roaringbitmap.longlong.Roaring64Bitmap;
//import java.util.Iterator;
//
//public class Bitmaptest {
//    public static void main(String[] args) {
//      RoaringBitmap r1 = RoaringBitmap.bitmapOf(15, 0, Integer.MIN_VALUE, Integer.MAX_VALUE, -1, -8);
//      System.out.println(r1+","+ "r1");
//      RoaringBitmap r2 = new RoaringBitmap();
//      System.out.println(r2+","+"r2");
//
//      RoaringBitmap intersect = RoaringBitmap.and(r1, r2);
//      System.out.println(intersect+","+"r1-and-r2");
//      System.out.println("=================");
//
//      Roaring64Bitmap r64 = Roaring64Bitmap.bitmapOf(1, 2, Long.MAX_VALUE, Long.MIN_VALUE, -1, -3);
//      System.out.println("RBM[r64] cardinality: " + r64.getLongCardinality());
//      Iterator<Long> iterator = r64.iterator();
//      while (iterator.hasNext()) {
//          System.out.print(iterator.next() + "\t");
//      }
//      System.out.println();
//    }
//}

package com.thinkbiganalytics.kylo.utils;

/*
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.spark.sql.types.*;
import org.junit.Assert;
import org.junit.Test;


import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import com.thinkbiganalytics.spark.service.AbstractDataSetConverterService;
import scala.Option;
*/
/*

import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaHiveDecimalObjectInspector, PrimitiveObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorConverters, ObjectInspectorFactory}
import org.apache.spark.sql.types._
import org.junit.{Assert, Test}
 */

public class TestObjectInspector {

    /*
    IntegerType$ integerTypeSingleton = IntegerType$.MODULE$;
    ArrayType arrayOfIntsType = ArrayType$.MODULE$.apply(integerTypeSingleton);

    private AbstractDataSetConverterService converterService = new AbstractDataSetConverterService() {
        @Override
        public Option<ObjectInspector> findHiveObjectInspector(DataType dataType) {
            return Option.empty();
        }

        @Override
        public Option<ObjectInspectorConverters.Converter> findHiveObjectConverter(DataType dataType) {
            return Option.empty();
        }
    };

    @Test
    public void testObjectInspector() {
        Assert.assertEquals(PrimitiveObjectInspectorFactory.javaIntObjectInspector, converterService.getHiveObjectInspector(integerTypeSingleton));
        Assert.assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector),
                converterService.getHiveObjectInspector(arrayOfIntsType));



        DataType smallDecimalType = new DecimalType(10, 0);
        JavaHiveDecimalObjectInspector smallDecimalInspector = (JavaHiveDecimalObjectInspector)converterService.getHiveObjectInspector(smallDecimalType);
        Assert.assertEquals(10, smallDecimalInspector.precision());
        Assert.assertEquals(0, smallDecimalInspector.scale());

    }
    */
}


/*
     Verify converting Spark SQL types to Hive object inspectors.
@Test
def toObjectInspector(): Unit = {
        // Test type conversions
        Assert.assertEquals(PrimitiveObjectInspectorFactory.javaIntObjectInspector, converterService.getHiveObjectInspector(IntegerType))
        Assert.assertEquals(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector),
        converterService.getHiveObjectInspector(ArrayType(IntegerType)))
        Assert.assertEquals(ObjectInspectorFactory.getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector),
        converterService.getHiveObjectInspector(MapType(StringType, IntegerType)))

        // Test decimal type conversion
        val smallDecimalType = new DecimalType(10, 0)
        val smallDecimalInspector = converterService.getHiveObjectInspector(smallDecimalType).asInstanceOf[JavaHiveDecimalObjectInspector]
        Assert.assertEquals(10, smallDecimalInspector.precision())
        Assert.assertEquals(0, smallDecimalInspector.scale())

        val largeDecimalType = new DecimalType()
        val largeDecimalInspector = converterService.getHiveObjectInspector(largeDecimalType).asInstanceOf[JavaHiveDecimalObjectInspector]
        Assert.assertEquals(10, largeDecimalInspector.precision())
        Assert.assertEquals(0, largeDecimalInspector.scale())

        // Test struct type conversion
        val dataType = StructType(Array(StructField("id", IntegerType)))
        val structOI = ObjectInspectorFactory.getStandardStructObjectInspector(util.Arrays.asList("id"), util.Arrays.asList(PrimitiveObjectInspectorFactory.javaIntObjectInspector))
        Assert.assertEquals(structOI, converterService.getHiveObjectInspector(dataType))
        }
 */
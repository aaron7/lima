package uk.ac.cam.cl.groupproject12.lima.hbase;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation used to identify the parts of an HBaseAutoWriter which represent parts of the key.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseKey 
{

}

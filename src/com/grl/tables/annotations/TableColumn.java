package com.grl.tables.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.grl.tables.ColumnSerializer;
import com.grl.tables.serializers.PrimitiveSerializer;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TableColumn {
	public String name();
	public String domain() default "default";
	public Class<? extends ColumnSerializer> serializer() default PrimitiveSerializer.class;
	public String format() default "";
}

package com.grl.tables.transforms;

import java.util.HashMap;
import java.util.Map;

import com.grl.tables.transforms.RowTransform.Mode;

public class RowTransformFactory {
	private Map<String,String> fields = new HashMap<String,String>();
	private Map<String,String> defaults = new HashMap<String,String>();
	private Map<String,ColumnTransform> colTransforms = new HashMap<String,ColumnTransform>();
	private Map<String,ColumnBuilder> colBuilders = new HashMap<String,ColumnBuilder>();
	
	public RowTransformFactory putField(String oldFieldName,String newFieldName){
		fields.put(oldFieldName,newFieldName);
		return this;
	}
	public RowTransformFactory putFieldAndDefault(String oldFieldName,String newFieldName,String defaultValue){
		putField(oldFieldName,newFieldName);
		defaults.put(newFieldName,defaultValue);
		return this;
	}
	public RowTransformFactory putFieldAndTransform(String oldFieldName,String newFieldName, ColumnTransform colTransform){
		putField(oldFieldName,newFieldName);
		colTransforms.put(newFieldName,colTransform);
		return this;
	}
	public RowTransformFactory putFieldBuilder(String newFieldName, ColumnBuilder colBuilder){
		defaults.remove(newFieldName);
		colBuilders.put(newFieldName, colBuilder);
		return this;
	}
	public RowTransform buildUnion(){
		RowTransform transform = new RowTransform(fields);
		transform.setColumnTransforms(colTransforms);
		transform.setColumnBuilders(colBuilders);
		transform.setFieldDefaults(defaults);
		transform.setMode(Mode.Union);
		return transform;
	}
	public RowTransform buildIntersection(){
		RowTransform transform = new RowTransform(fields);
		transform.setColumnTransforms(colTransforms);
		transform.setColumnBuilders(colBuilders);
		transform.setFieldDefaults(defaults);
		transform.setMode(Mode.Intersection);
		return transform;
	}
}

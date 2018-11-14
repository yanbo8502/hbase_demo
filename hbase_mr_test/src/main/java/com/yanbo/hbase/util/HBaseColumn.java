package com.yanbo.hbase.util;

public class HBaseColumn
{
	private String family;
	private String qualifier;
	
	public HBaseColumn(String _family, String _qualifier)
	{
		family = _family;
		qualifier = _qualifier;
	}
	
	public String getFamily()
	{
		return family;
	}
	
	public String getQualifier()
	{
		return qualifier;
	}
}
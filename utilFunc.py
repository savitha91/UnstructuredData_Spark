#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 14:30:10 2020

@author: sarames
"""

class utilFunc:
	def __init__(self):
		pass

	def removeComma(self,line):
		if(',' in line):
			line = line.replace(",","")
		return line

	def replaceDoubleColon(self,line):
		if('::' in line):
			line = line.replace("::","|")
		return line

	def addEmptyValToMissingCol(self,line,maxLen):
		if(len(line) < maxLen):
			self.repeatCode(len(line),line,maxLen)
		return line

	def repeatCode(self,i,line,maxLen):
		while True:
			line.append("")
			i=i+1
			if (i==maxLen):
				break



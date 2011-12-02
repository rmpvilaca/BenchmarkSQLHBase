from pyx import *
from statlib import stats
import sys

def main(args):
		types=["Payment", "New-Order", "Delivery","Order-Status","Stock-Level"]
		outT=open("TPCC-throughput.dat", 'w')
		outL=open("TPCC-latency.dat", 'w')
		clients=[1,10,20,30,40]
		if len(clients)!=(len(args)-1):
			print "The size of clients it not compatible with the list of files."
			sys.exit(0)
		for i in range(1,len(args)):
			fin=args[i]
			startLine=False
			notEndLine=True
			throughput=0
			mapLatency={}
			print fin, clients[i-1]
			for t in types:
   				mapLatency[t]=[]
   			mapLatency["total"]=[]
   			f = open(fin, 'r')
   			lines = f.readlines()
   			for line in lines:
   				sep=line.split('\t')
   				if line.startswith("Transaction Number"):
   					startLine=True
   				elif line.startswith("Measured tpmC"):
   					expr=line.split("=")[1]
   					tmp1=expr.split("/")
   					tmp2=tmp1[0].split("*")
					throughput=float(tmp2[0])*float(tmp2[1])/float(tmp1[1])
   				elif startLine and line=="\n":
   						notEndLine=False
   				elif startLine and notEndLine:
					for t in types:
   						if t==sep[2]:
   							mapLatency[t].append(float(sep[3]))
   					mapLatency["total"].append(float(sep[3]))
   			print >>outT, clients[i-1],throughput
   			print >>outL, clients[i-1],
   			for t in types:
   				print >>outL,stats.mean(mapLatency[t]),
   			print >>outL, 	stats.mean(mapLatency["total"])	
   			f.close()
		outT.close()
		outL.close()
		inT="TPCC-throughput"
		inL="TPCC-latency"
		mColor=[color.cmyk.Green,color.cmyk.Orange ,color.cmyk.Red,color.cmyk.Violet,color.cmyk.Blue,color.cmyk.Yellow,color.cmyk.Sepia ,color.cmyk.Brown, color.cmyk.Gray, color.cmyk.Black]
		gT =graph.graphxy(width=8, x=graph.axis.linear(min=0,max=50,title="Clients"),  y=graph.axis.linear(min=0,max=2000,title="Throughput (ops/min)"))
		gT.plot([graph.data.file(inT+".dat", x=1, y=2)],[graph.style.symbol(symbolattrs=[attr.changelist(mColor)]),graph.style.line([attr.changelist(mColor),style.linewidth.Thick])])
		gT.writePDFfile(inT)
		gT.writeEPSfile(inT)
		gL =graph.graphxy(width=8,key=graph.key.key(pos="tl", dist=0.1), x=graph.axis.linear(min=0,max=50,title="Clients"),  y=graph.axis.linear(title="Latency (ms)"))
		lL=[]
		i=2
		for t in types:
			lL.append(graph.data.file(inL+".dat", x=1, y=i,title=t))
			i+=1
		lL.append(graph.data.file(inL+".dat", x=1, y=i,title="Total"))
		gL.plot(lL,[graph.style.symbol(symbolattrs=[attr.changelist(mColor)]),graph.style.line([attr.changelist(mColor),style.linewidth.Thick])])
		gL.writePDFfile(inL)
		gL.writeEPSfile(inL)
main(sys.argv)
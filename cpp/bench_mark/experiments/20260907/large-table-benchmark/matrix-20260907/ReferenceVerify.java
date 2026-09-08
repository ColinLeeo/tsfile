/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.File;
import java.util.*;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.v4.*;
import org.apache.tsfile.read.query.dataset.ResultSet;

public class ReferenceVerify {
  static final long EPOCH=1700000000000L, ROWS=200000000L;
  static long mix(long x) { x^=x>>>30;x*=0xbf58476d1ce4e5b9L;x^=x>>>27;x*=0x94d049bb133111ebL;return x^(x>>>31); }
  static void check(boolean b,String m) { if(!b)throw new AssertionError(m); }
  public static void main(String[] args) throws Exception {
    long points=0; int devices=0;
    try(TsFileSequenceReader seq=new TsFileSequenceReader(args[0])) {
      for(var entry:seq.getAllTimeseriesMetadata(false).entrySet()) {
        devices++;int columns=0;
        for(var meta:entry.getValue()) {
          if(meta.getMeasurementId().isEmpty())continue;
          columns++;
          check(meta.getStatistics().getCount()==ROWS,"metadata count");
          check(meta.getStatistics().getStartTime()==EPOCH && meta.getStatistics().getEndTime()==EPOCH+ROWS-1,"metadata time");
          points+=meta.getStatistics().getCount();
        }
        check(columns==5,"metadata columns");
      }
    }
    check(devices==10 && points==10000000000L,"metadata totals");
    long checked=0;
    try(ITsFileReader reader=new TsFileReaderBuilder().file(new File(args[0])).build()) {
      for(long[] range:new long[][]{{0,2},{9998,10002},{ROWS/2,ROWS/2+2},{ROWS-10000,ROWS-1}}) {
        long[] next=new long[10];Arrays.fill(next,range[0]);
        try(ResultSet rs=reader.query("benchmark",Arrays.asList("device","i32","i64","f32","f64","i64_2"),EPOCH+range[0],EPOCH+range[1])) {
          while(rs.next()) {
            long row=rs.getLong(1)-EPOCH;String tag=rs.getString(2);int d=tag.charAt(8)-'0';
            check(d>=0&&d<10&&row==next[d]++&&row<=range[1],"key");
            for(int c=2;c<=7;c++)check(!rs.isNull(c),"NULL");
            long a=mix(row+0x9e3779b97f4a7c15L*(d+1));long b=mix(a+0x123456789abcdefL);
            check(rs.getInt(3)==(int)Long.remainderUnsigned(a,2000001)-1000000,"i32 "+d+":"+row);
            check(rs.getLong(4)==1000000000000L+row*100+d,"i64 "+d+":"+row);
            check(rs.getFloat(5)==((int)((a>>>32)&0xffffff)-0x800000)/256.0f,"float "+d+":"+row);
            check(rs.getDouble(6)==(double)(b>>>12)/1048576.0,"double "+d+":"+row);
            check(rs.getLong(7)==(b&0x7fffffffffffffffL),"i64_2 "+d+":"+row+" actual="+rs.getLong(7)+" expected="+(b&0x7fffffffffffffffL));
            checked++;
          }
        }
        for(long n:next)check(n==range[1]+1,"missing sample");
      }
    }
    System.out.println("REFERENCE_VERIFICATION_PASSED devices="+devices+" metadata_numeric_points="+points+" checked_rows="+checked);
  }
}

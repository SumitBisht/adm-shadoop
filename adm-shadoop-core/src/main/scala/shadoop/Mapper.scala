/**
 * Copyright (C) 2013 Adam Retter (adam.retter@googlemail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package shadoop


import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.{Writable, WritableComparable}


import org.apache.hadoop.mapreduce.{Mapper => HMapper}
import org.apache.hadoop.hbase.mapreduce.{TableMapper => HTableMapper}

trait MP[KIN, VIN, KOUT, VOUT] extends MapReduceConfig {
  type ContextType = HMapper[KIN, VIN, KOUT, VOUT]#Context
  type MapperType = (KIN, VIN, ContextType) => Iterable[(KOUT, VOUT)]
  var mapper: Option[MapperType] = None

  def kType: Class[_]
  def vType: Class[_]
  def mapWith(f: MapperType) = mapper = Some(f)
}


abstract class Mapper[KIN, VIN, KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HMapper[KIN, VIN, KOUT, VOUT] with MP[KIN, VIN, KOUT, VOUT] with OutTyped[KOUT, VOUT] {


  def kType = kTypeM.runtimeClass.asInstanceOf[Class[KOUT]]
  def vType = vTypeM.runtimeClass.asInstanceOf[Class[VOUT]]

	override def map(k: KIN, v: VIN, context: ContextType): Unit = {
    debug("running mapper with context "+context)
		mapper.map(func => func(k, v, context).map(pair => context.write(pair._1, pair._2)))
  }

  override def setup(context: ContextType) {
    debug("setting up configuration for mapper")
    this.configuration = Option(context.getConfiguration)
  }

}

abstract class TableMapper[KOUT, VOUT](implicit kTypeM: Manifest[KOUT], vTypeM: Manifest[VOUT])
  extends HTableMapper[KOUT, VOUT] with MP[ImmutableBytesWritable, Result, KOUT, VOUT] with TableOutTyped[KOUT, VOUT] {

  def kType = kTypeM.runtimeClass.asInstanceOf[Class[WritableComparable[KOUT]]]
  def vType = vTypeM.runtimeClass.asInstanceOf[Class[Writable]]

  override def map(k: ImmutableBytesWritable, v: Result, ctx: ContextType): Unit = {
    debug("running table mapper with context "+ctx)
    mapper.map(func => func(k, v, ctx).map(pair => ctx.write(pair._1, pair._2)))
  }

  override def setup(ctx: ContextType): Unit = {
    debug("setting up configuration for table mapper")
    this.configuration = Option(ctx.getConfiguration)
  }

}


trait LocalCacheFileWrapper {
  var configuration: Option[Configuration]
  def getLocalCacheFiles = DistributedCache.getCacheFiles(configuration.get)
  def useCacheFile(fileUri: URI): Unit
  def process() = {
    getLocalCacheFiles.foreach(useCacheFile)
  }
}
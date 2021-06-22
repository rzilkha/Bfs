@file:JvmName("SparkBfs")

import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.*

import org.apache.spark.sql.functions.flatten
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.flatMap

const val DONE = 2
const val PENDING = 0
const val READY = 1

fun ifElseColumn(column: String, compareValue: Any, value: Any?, elseValue: Any?): Column {
    return `when`(col(column).equalTo(compareValue), value ?: lit(null))
        .otherwise(elseValue ?: lit(null))
}

fun explodeConnections(iteration: Int): (Row) -> Iterator<Row> {
    return {
        val name = it.getString(0)
        val connections = it.getList<String>(1)
        val distance = it.get(2)
        val status = it.getInt(3)
        val normalized = mutableListOf<Row>()
        if (status == READY) {
            for (connection in connections) {
                normalized.add(RowFactory.create(connection, arrayOf<String>(), iteration, READY))
            }
            normalized.add(RowFactory.create(name, connections.toTypedArray(), distance, DONE))
        } else {
            normalized.add(it)
        }
        normalized.iterator()
    }
}

fun main(args: Array<String>) {

    val fileName = args[0]
    val source = args[1]
    val iterations = Integer.valueOf(args[2])
    val sparkSession = SparkSession.builder().appName("BFS").master("local[*]")
        .orCreate
    val dataframe = sparkSession.read()
        .option("header", true)
        .csv(fileName)

    val schema: StructType? = StructType()
        .add(StructField.apply("name", DataTypes.StringType, true, null))
        .add(StructField.apply("connections", ArrayType.apply(DataTypes.StringType), true, null))
        .add(StructField.apply("distance", DataTypes.IntegerType, true, null))
        .add(StructField.apply("status", DataTypes.IntegerType, true, null))


    var data = dataframe.select(col("name"), col("friend"))
        .groupBy(col("name"))
        .agg(collect_set(col("friend")).`as`("connections"))
        .withColumn(
            "distance",
            ifElseColumn("name", source, 0, null)
        )
        .withColumn(
            "status",
            ifElseColumn("name", source, READY, PENDING)
        )


    val rowEncoder: ExpressionEncoder<Row> = RowEncoder.apply(schema)



    for (i in 1..iterations) {
//

        data = data.flatMap(explodeConnections(i), rowEncoder)
        //  group all ready rows with other rows, grouped by name
        data = data.select("*").groupBy(col("name"))
            .agg(
                flatten(collect_set(col("connections"))).`as`("connections"),
                min(col("distance")).`as`("distance"),
                max(col("status")).`as`("status")
            )

//        }
    }

    data.select("name","distance").where(col("distance").isNotNull)
        .write()
        .mode("overwrite")
        .csv("output.csv")
    sparkSession.close()
}


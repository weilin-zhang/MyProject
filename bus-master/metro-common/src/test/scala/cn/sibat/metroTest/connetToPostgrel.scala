package cn.sibat.metroTest

import java.sql.{DriverManager, ResultSet};

/**
  * 连接到postgrel
  * Created by wing1995 on 2017/8/22.
  */
object connetToPostgrel{

    val connAddress = "jdbc:postgresql://192.168.40.113:5432/szt_green"
    val userName = "szt"
    val password = "szt123"

    def main(args: Array[String]) {
        val conn = DriverManager.getConnection(connAddress, userName, password)
        try {
            // Configure to be Read Only
            val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

            // Execute Query
            val rs = statement.executeQuery("SELECT * FROM card_record")
            val columnCount = rs.getMetaData.getColumnCount
            // Iterate Over ResultSet
            while (rs.next) {
                for (i <- 1 to columnCount) {
                    System.out.print(rs.getString(i) + "\t")
                }
                System.out.println()
            }
        } finally {
            conn.close()
        }
    }
}

import sbt._
import sbt.Keys._
import sbt.{Compile, TaskKey}

import java.io.{File, IOException}
import java.nio.file.Files

object CopyDependencies {

  val copyDeps = TaskKey[Unit]("copyDeps", "Copies needed dependencies to the build directory.")
  val destPath = (Compile / crossTarget) { _ / "jars"}

  lazy val settings = Seq(
    copyDeps := {
      val dest = destPath.value
      if (!dest.isDirectory && !dest.mkdirs()) {
        throw new IOException("Failed to create jars directory.")
      }

      (Compile / dependencyClasspath).value.map(_.data)
        .filter { jar => jar.isFile }
        .foreach { jar =>
          val destJar = new File(dest, jar.getName)
          if (destJar.isFile) {
            destJar.delete()
          }
          Files.copy(jar.toPath, destJar.toPath)
        }
    },
    (Compile / packageBin / crossTarget) := destPath.value,
    (Compile / packageBin) := (Compile / packageBin).dependsOn(copyDeps).value
  )

}
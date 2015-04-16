package com.ctrip.hermes.meta.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.unidal.lookup.annotation.Named;

@Named
public class CompileService {

	private JavaCompiler jdkCompiler = ToolProvider.getSystemJavaCompiler();

	public void compile(final Path destDir) throws IOException {
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				int result = jdkCompiler.run(null, null, null, "-d", destDir.toAbsolutePath().toString(), "-classpath",
				      System.getProperty("java.class.path"), path.toAbsolutePath().toString());
				if (result != 0) {
					return FileVisitResult.TERMINATE;
				}
				return FileVisitResult.CONTINUE;
			}

		});
	}

	public void jar(final Path destDir, Path jarFile) throws IOException {
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		final JarOutputStream target = new JarOutputStream(new FileOutputStream(jarFile.toFile()), manifest);
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				File file = path.toFile();
				Path pathRelative = destDir.relativize(path);
				String name = pathRelative.toString().replace("\\", "/");
				JarEntry entry = new JarEntry(name);
				entry.setTime(file.lastModified());
				target.putNextEntry(entry);
				byte[] readAllBytes = Files.readAllBytes(file.toPath());
				target.write(readAllBytes);
				target.closeEntry();
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attrs) throws IOException {
				File file = path.toFile();
				Path pathRelative = destDir.relativize(path);
				String name = pathRelative.toString().replace("\\", "/");
				if (!name.isEmpty()) {
					if (!name.endsWith("/"))
						name += "/";
					JarEntry entry = new JarEntry(name);
					entry.setTime(file.lastModified());
					target.putNextEntry(entry);
					target.closeEntry();
				}
				return super.preVisitDirectory(path, attrs);
			}

		});
		target.close();
	}

	public void delete(final Path destDir) throws IOException {
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				Files.delete(path);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return super.postVisitDirectory(dir, exc);
			}

		});
	}
}

package com.ctrip.hermes.meta.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.unidal.helper.Joiners;
import org.unidal.lookup.annotation.Named;

@Named
public class CompileService {

	private JavaCompiler jdkCompiler = ToolProvider.getSystemJavaCompiler();

	/**
	 * 
	 * @param destDir
	 * @throws IOException
	 */
	public void compile(final Path destDir) throws IOException {
		final Set<String> filesPath = new HashSet<String>();
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (file.toString().endsWith(".java"))
					filesPath.add(file.getParent().toAbsolutePath().toString() + "\\*.java");
				return super.visitFile(file, attrs);
			}

		});

		System.out.println(String.format("javac -d %s -classpath %s %s", destDir.toAbsolutePath().toString(),
		      System.getProperty("java.class.path"), Joiners.by(" ").join(filesPath)));
		jdkCompiler.run(System.in, System.out, System.err, "-d", destDir.toAbsolutePath().toString(), "-classpath",
		      System.getProperty("java.class.path"), Joiners.by(" ").join(filesPath));
	}

	/**
	 * 
	 * @param destDir
	 * @param jarFile
	 * @throws IOException
	 */
	public void jar(final Path destDir, Path jarFile) throws IOException {
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR, "com.ctrip");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_TITLE, "Avro Schema");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR_ID, "com.ctrip");
		manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_VENDOR, "com.ctrip");
		manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_TITLE, "Avro Schema");
		manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_VERSION, "");
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
				byte[] readAllBytes = Files.readAllBytes(path);
				target.write(readAllBytes);
				target.closeEntry();
				return super.visitFile(path, attrs);
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

	/**
	 * 
	 * @param destDir
	 * @throws IOException
	 */
	public void delete(final Path destDir) throws IOException {
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				Files.delete(path);
				return super.visitFile(path, attrs);
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return super.postVisitDirectory(dir, exc);
			}

		});
	}
}

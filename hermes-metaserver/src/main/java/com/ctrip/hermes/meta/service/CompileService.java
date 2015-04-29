package com.ctrip.hermes.meta.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.unidal.lookup.annotation.Named;

@Named
public class CompileService {

	private JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

	/**
	 * 
	 * @param destDir
	 * @throws IOException
	 */
	public void compile(final Path destDir) throws IOException {
		final List<File> files = new ArrayList<File>();
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (file.toString().endsWith(".java"))
					files.add(file.toFile());
				return super.visitFile(file, attrs);
			}

		});

		StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(files);
		compiler.getTask(null, fileManager, diagnostics, null, null, compilationUnits).call();
		for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics())
			System.out.format("%s on line %d in %s: %s%n", diagnostic.getKind().toString(), diagnostic.getLineNumber(),
			      diagnostic.getSource().toUri(), diagnostic.getMessage(null));
		fileManager.close();
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

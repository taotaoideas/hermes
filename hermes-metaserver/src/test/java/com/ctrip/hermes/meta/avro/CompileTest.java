package com.ctrip.hermes.meta.avro;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.service.CompileService;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class CompileTest extends ComponentTestCase {

	@Test
	public void testCompileUBT() throws IOException {
		String schemaContent = Files.toString(new File("src/test/resources/ubt/mmetric.avsc"), Charsets.UTF_8);
		Parser parser = new Parser();
		Schema avroSchema = parser.parse(schemaContent);
		SpecificCompiler compiler = new SpecificCompiler(avroSchema);

		CompileService compileService = lookup(CompileService.class);
		Path ubtDir = new File("target/test/java/").toPath();

		compiler.compileToDestination(null, ubtDir.toFile());
		Path compileDir = new File("target/test/java/" + avroSchema.getNamespace().replace('.', '/')).toPath();
		compileService.compile(compileDir);
		Path jarFile = java.nio.file.Files.createTempFile("ubt", ".jar");
		compileService.jar(ubtDir, jarFile);
		System.out.println(jarFile);
		jarFile.toFile().delete();
		compileService.delete(ubtDir);
	}
}

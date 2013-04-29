package julien.galago.tupleflow.typebuilder;

import org.antlr.runtime.RecognitionException;

import julien.galago.tupleflow.typebuilder.Direction;
import julien.galago.tupleflow.typebuilder.TypeSpecification;
import julien.galago.tupleflow.typebuilder.FieldSpecification.DataType;
import junit.framework.TestCase;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;

/**
 * @author trevor
 */
public class ParserTest extends TestCase {
    public void testParser() throws RecognitionException {
        String template =
                "package julien.galago.core.types; \n" +
                "type DocumentExtent {\n" +
                "  bytes extentName;\n" +
                "  long document;\n" +
                "  int begin;\n" +
                "  int end;\n" +
                "  order: +extentName +document ;\n" +
                "  order: ;\n" +
                "}";

        ANTLRStringStream input = new ANTLRStringStream(template);
        GalagoTypeBuilderLexer lexer = new GalagoTypeBuilderLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        GalagoTypeBuilderParser parser = new GalagoTypeBuilderParser(tokens);
        TypeSpecification spec = parser.type_def();

        assertEquals("DocumentExtent", spec.getTypeName());
        assertEquals("julien.galago.core.types", spec.getPackageName());

        assertEquals("extentName", spec.getFields().get(0).name);
        assertEquals(DataType.BYTES, spec.getFields().get(0).type);

        assertEquals("document", spec.getOrders().get(0).getOrderedFields().get(1).name);
        assertEquals(Direction.ASCENDING,
                spec.getOrders().get(0).getOrderedFields().get(1).direction);
    }
}

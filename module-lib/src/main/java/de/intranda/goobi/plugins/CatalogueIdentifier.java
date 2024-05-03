package de.intranda.goobi.plugins;

import lombok.ToString;
import lombok.Value;

@Value
@ToString
public class CatalogueIdentifier {
    String field;
    String searchValue;
}

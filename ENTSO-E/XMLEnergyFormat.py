import xml.dom.minidom

# Open the XML file
with open('C:\\Users\\jeppe\\Documents\\GitHub\\REO\\ENTSO-E\\FInuclear.xml', 'r') as file:
    xml_data = file.read()

# Parse the XML data
dom = xml.dom.minidom.parseString(xml_data)

# Pretty print the XML content
pretty_xml = dom.toprettyxml(indent='  ')

# Write the formatted XML to a file
with open('FINuclearFormat.xml', 'w') as file:
    file.write(pretty_xml)




#TEST FORMATTING OLD????
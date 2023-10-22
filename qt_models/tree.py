""" Base classes for QtTreeModel implementation """
from PyQt5.QtWidgets import QTreeView
from qgis.PyQt import QtCore

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtGui import QBrush
from legger.utils.formats import transform_none
from legger.utils.formats import try_round

CHECKBOX_FIELD = 1
INDICATION_HOVER = 2
INDICATION_HOVER_FUNCTION = 3


class BaseTreeItem(object):
    """
    a python object used to return row/column data, and keep note of
    it's parents and/or children
    """

    def __init__(self, data_item, parent, headers=None):

        if headers is None:
            headers = []
        self.headers = headers
        self.data_item = data_item
        self.parent_item = parent
        self.childs = []

    def __repr__(self):
        return "%s - %i childs" % (self.data_item, len(self.childs))

    def appendChild(self, item):
        self.childs.append(item)
        if item.parent() != self:
            item.setParent(self)

    def insertChild(self, index, item):
        self.childs.insert(index, item)
        if item.parent() != self:
            item.setParent(self)

    def clearChilds(self):
        self.childs = []

    def setParent(self, parent_item):
        self.parent_item = parent_item
        if self not in parent_item.childs:
            self.appendChild(self)

    def child(self, row):
        return self.childs[row]

    def childCount(self):
        return len(self.childs)

    def columnCount(self):
        return len(self.headers)

    def data(self, column, qvalue=False):
        if self.data_item is None:
            if column == 0:
                return 'root'
            if column == 1:
                return ""
        else:
            value = self.data_item.data(column, qvalue)
            if self.headers[column].get('round') is not None:
                value = try_round(value, self.headers[column].get('round'))
            return value

        return None

    def setData(self, column, value, role, signal=True):
        ret = self.data_item.setData(column, value, role)
        # if signal and ret:
        #     if self.item.model:
        #         index = self.item.model.index(
        #             self.item.get_row_nr(), self.field.column_nr)
        #         self.item.model.dataChanged.emit(index, index)
        return ret

    def icon(self):
        return self.data_item.get('icon')

    def parent(self):
        return self.parent_item

    def row(self):
        if self.parent_item:
            return self.parent_item.childs.index(self)
        return 0


class BaseTreeModel(QtCore.QAbstractItemModel):
    """
    Base tree model

    """

    def __init__(self, parent=None, root_item=None,
                 item_class=BaseTreeItem, headers=None):
        super(BaseTreeModel, self).__init__(parent)

        if headers is None:
            headers = []

        self.headers = headers
        self.headers_dict = dict([(h['field'], h) for h in headers])
        self.total_width = sum([max(h.get('column_width', 70), 70) for h in headers if h.get('show', True)])

        self.item_class = item_class
        if root_item:
            self.rootItem = root_item
        else:
            self.rootItem = item_class(None, None)
        self.parents = {0: self.rootItem}
        self.tree_widget = None

    def setTreeWidget(self, widget):
        """
        set reference to tree widget, to make it possible to check visual state

        widget (QTreeWidget): Treewidget
        return: None
        """
        self.tree_widget = widget

    def columnCount(self, parent=None):
        """
        get column count (required QAbstractItemModel function)

        parent (QModelIndex): Index of one of the rows
        return (int): number of columns (-1 if parent is not valid)
        """
        if parent and parent.isValid():
            return parent.internalPointer().columnCount()
        else:
            return len(self.headers)

    def data(self, index, role):
        """
        get data of field (required QAbstractItemModel function)

        index (QModelIndex): Index of row and field
        role (QtRole): type of data returned (value or style attribute)
        return (could be everything): value or None of role is not supported
        """

        if not index.isValid():
            return None

        item = index.internalPointer()
        if role == QtCore.Qt.DisplayRole:
            if self.headers[index.column()].get('field_type') in (INDICATION_HOVER, INDICATION_HOVER_FUNCTION):
                if item.data(index.column()):
                    return '*'
                else:
                    return None
            elif self.headers[index.column()].get('field_type') != CHECKBOX_FIELD:
                return item.data(index.column())
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignVCenter
        elif role == Qt.CheckStateRole:
            if self.headers[index.column()].get('field_type') == CHECKBOX_FIELD:
                return item.data(index.column(), qvalue=True)
            else:
                return None
        elif role == Qt.BackgroundRole:
            return QBrush(Qt.red)
            # return QBrush(Qt.transparent)
        # elif role == QtCore.Qt.DecorationRole and index.column() == 0:
        #     return item.icon()
        elif role == Qt.ToolTipRole:
            if self.headers[index.column()].get('field_type') == INDICATION_HOVER:
                return item.data(index.column())
            elif self.headers[index.column()].get('field_type') == INDICATION_HOVER_FUNCTION:
                return self.headers[index.column()].get('hover_function')(item, index.column())
        elif role == QtCore.Qt.UserRole:
            if item:
                return item
        return None

    def setData(self, index, value, role=Qt.DisplayRole, signal=True):
        """
        set data for specified index (row and field), including sending of signals (required QAbstractItemModel function)

        index (QtModelIndex): index to row and field
        value (could be everything): new value for ItemField
        role (Qt role): currently only DisplayRole or CheckStateRole are supported
        signal (bool): send signal
        return: was setting value successful
        """
        if not index.isValid():
            return None

        item = index.internalPointer()

        # dataChanged.emit is done within the ItemField, triggered by setting the value
        # todo: add check on Qt Role
        changed = item.setData(index.column(), value, role)
        if changed:
            if self.headers[index.column()].get('single_selection') and value in [True, Qt.Checked]:
                self.set_column_value(index.column(), Qt.Unchecked, skip=index)
            # hook to implement custom logic
            self.data_change_post_process(index, index)

            if signal:
                self.dataChanged.emit(index, index)
        return changed

    def get_column_nr(self, key):
        """
        get column number of field name

        key (str): field name
        return (int): column number. Exception is raised when field is not found.
        """
        header = self.headers_dict.get(key)
        column_nr = self.headers.index(header)
        return column_nr

    def setDataItemKey(self, item, key, value, role=Qt.DisplayRole, signal=True):
        """
        set data of specified item and  field name (key).

        item (AreaTreeItem): tree item of which the field must be modified
        key (str): field name
        value (could be everything): new value
        role (QtRole): kind of data set
        signal (bool): send signal
        return: None
        """
        try:
            column_nr = self.get_column_nr(key)
        except ValueError:
            raise ValueError('Key {} niet gevonden in kolom configuratie'.format(key))
        index = self.createIndex(item.row(), column_nr, item)
        self.setData(index, value, role, signal)

    def set_column_value(self, column, value, skip=None, signal=True):
        """
        set all values in column to this value

        column (int or string): column number or field_name
        value: new value for column
        skip (QModelIndex): index of item which will be skipped in setting value
        return (bool): if there are fields changed
        """

        if type(column) != int:
            column = self.headers.index(self.headers_dict.get(column))

        def loop_nodes(node):
            """
            a function called recursively, looking at all nodes beneath node
            """
            changed = False
            for child in node.childs:
                index = self.createIndex(child.row(), column, child)
                if index != skip:
                    changed = self.setData(index, value, signal=signal)

                if child.childCount() > 0:
                    changed_child = loop_nodes(child)
                    changed = changed or changed_child
            return changed

        changed = loop_nodes(self.parents[0])
        return changed

    def setNewTree(self, root_children):
        """
        replace tree with new tree

        root_children (list of AreaTreeItem): list of children of root element
        return:None
        """
        self.clear()
        self.beginInsertRows(QtCore.QModelIndex(), 0, len(root_children))
        for child in root_children:
            self.rootItem.appendChild(child)
        self.endInsertRows()

    def flags(self, index):
        """
        flags of item field (required QAbstractItemModel function)

        index (QModelIndex): Index of item and field
        return (Qt flag): flags
        """

        flags = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if self.headers[index.column()].get('field_type') == CHECKBOX_FIELD:
            flags |= Qt.ItemIsUserCheckable | Qt.ItemIsEditable

        return flags

    def clear(self):
        """
        clear tree

        return: None
        """
        self.beginRemoveRows(QtCore.QModelIndex(), 0, self.rootItem.childCount())
        self.rootItem.clearChilds()
        self.endRemoveRows()

    def headerData(self, column, orientation, role):
        """
        get header data. Currently only column titles for horizontal headers.

        column (int): column number
        orientation (Qt orientation): Qt orientation (Qt.Horizontal or Qt.Vertical)
        role (Qt role): type of data returned (value or style attribute)
        return:
        """
        if (orientation == QtCore.Qt.Horizontal and
                role == QtCore.Qt.DisplayRole):
            try:
                return self.headers[column].get(
                    'header', self.headers[column]['field'])
            except IndexError:
                pass
        return None

    def index(self, row, column, parent):
        """
        create index for this model

        row (int): row number
        column (int): column number
        parent (ModelItem): parent in the tree structure
        return (QModelIndex): model index
        """

        if not self.hasIndex(row, column, parent):
            return QtCore.QModelIndex()

        if not parent.isValid():
            parentItem = self.rootItem
        else:
            parentItem = parent.internalPointer()

        childItem = parentItem.child(row)
        if childItem:
            return self.createIndex(row, column, childItem)
        else:
            return QtCore.QModelIndex()

    def parent(self, index):
        """
        get index of parent of item

        index (QModelIndex): Index of item
        return (QModelIndex):  Index of parent. Empty index when
            there is no parent or item index is invalid
        """
        if not index.isValid():
            return QtCore.QModelIndex()

        childItem = index.internalPointer()
        if not childItem:
            return QtCore.QModelIndex()

        parentItem = childItem.parent()

        if transform_none(parentItem) is None:
            return QtCore.QModelIndex()

        if parentItem == self.rootItem:
            return QtCore.QModelIndex()

        return self.createIndex(parentItem.row(), 0, parentItem)

    def rowCount(self, parent=QtCore.QModelIndex()):
        """
        get number of childs

        parent (QModelIndex): index of item
        return (int): number of childs
        """
        if parent.column() > 0:
            return 0
        if not parent.isValid():
            p_item = self.rootItem
        else:
            p_item = parent.internalPointer()
        return p_item.childCount()

    # def searchModel(self, hydrovak):
    #     """
    #     get the modelIndex for a given
    #     """
    #
    #     def searchNode(node):
    #         """
    #         a function called recursively, looking at all nodes beneath node
    #         """
    #         for child in node.childs:
    #             if hydrovak == child.hydrovak:
    #                 index = self.createIndex(child.row(), 0, child)
    #                 return index
    #
    #             if child.childCount() > 0:
    #                 result = searchNode(child)
    #                 if result:
    #                     return result
    #
    #     retarg = searchNode(self.parents[0])
    #     return retarg

    def column(self, column_nr):
        """
        get column configuration
        column_nr (int): column number
        return (dict): dictionary with column configuration
        """
        return self.headers[column_nr]

    @property
    def columns(self):
        """
        get list with configuration of all column

        return (list): list with all column configurations
        """
        return self.headers

    def set_column_sizes_on_view(self, tree_view: QTreeView, width: float = None):
        """
        Helper function for applying the column sizes on a view.

        table_view (QTableView): table view instance that uses this model
        """

        factor = 1
        if width and width > self.total_width:
            # minus scrollbar
            factor = ((width - 40) / self.total_width)

        for i, col in enumerate(self.headers):
            width = max(col.get('column_width', 70), 70)
            show = col.get('show', True)
            if show:
                tree_view.setColumnWidth(i, round(float(width) * factor))
            else:
                tree_view.setColumnHidden(i, True)

    def data_change_post_process(self, index, to_index):
        """
        Hook for specific functions after changing values

        index (QModelIndex):
        to_index (QModelIndex):
        return: None
        """
        pass

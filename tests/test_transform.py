from pipe_template.transform import AddField


class TestTransform:

    def test_addfield(self):
        dofn = AddField(field='tag', value='test')
        assert list(dofn.process({})) == [{'tag': 'test'}]

